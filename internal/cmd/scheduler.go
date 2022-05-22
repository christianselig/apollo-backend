package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/christianselig/apollo-backend/internal/cmdutil"
	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/repository"
)

const batchSize = 250

func SchedulerCmd(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scheduler",
		Args:  cobra.ExactArgs(0),
		Short: "Schedules jobs and runs several maintenance tasks periodically.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := cmdutil.NewLogrusLogger(false)

			statsd, err := cmdutil.NewStatsdClient()
			if err != nil {
				return err
			}
			defer statsd.Close()

			db, err := cmdutil.NewDatabasePool(ctx, 1)
			if err != nil {
				return err
			}
			defer db.Close()

			redis, err := cmdutil.NewRedisClient(ctx)
			if err != nil {
				return err
			}
			defer redis.Close()

			queue, err := cmdutil.NewQueueClient(logger, redis, "worker")
			if err != nil {
				return err
			}

			// Eval lua so that we don't keep parsing it
			luaSha, err := evalScript(ctx, redis)
			if err != nil {
				return err
			}

			notifQueue, err := queue.OpenQueue("notifications")
			if err != nil {
				return err
			}

			subredditQueue, err := queue.OpenQueue("subreddits")
			if err != nil {
				return err
			}

			trendingQueue, err := queue.OpenQueue("trending")
			if err != nil {
				return err
			}

			userQueue, err := queue.OpenQueue("users")
			if err != nil {
				return err
			}

			stuckNotificationsQueue, err := queue.OpenQueue("stuck-notifications")
			if err != nil {
				return err
			}

			s := gocron.NewScheduler(time.UTC)
			_, _ = s.Every(500).Milliseconds().SingletonMode().Do(func() { enqueueAccounts(ctx, logger, statsd, db, redis, luaSha, notifQueue) })
			_, _ = s.Every(500).Milliseconds().SingletonMode().Do(func() { enqueueSubreddits(ctx, logger, statsd, db, []rmq.Queue{subredditQueue, trendingQueue}) })
			_, _ = s.Every(500).Milliseconds().SingletonMode().Do(func() { enqueueUsers(ctx, logger, statsd, db, userQueue) })
			_, _ = s.Every(1).Second().Do(func() { cleanQueues(logger, queue) })
			_, _ = s.Every(1).Second().Do(func() { enqueueStuckAccounts(ctx, logger, statsd, db, stuckNotificationsQueue) })
			_, _ = s.Every(1).Minute().Do(func() { reportStats(ctx, logger, statsd, db) })
			_, _ = s.Every(1).Minute().Do(func() { pruneAccounts(ctx, logger, db) })
			_, _ = s.Every(1).Minute().Do(func() { pruneDevices(ctx, logger, db) })
			s.StartAsync()

			<-ctx.Done()

			s.Stop()

			return nil
		},
	}

	return cmd
}

func evalScript(ctx context.Context, redis *redis.Client) (string, error) {
	lua := fmt.Sprintf(`
		local retv={}
		local ids=cjson.decode(ARGV[1])

		for i=1, #ids do
			local key = KEYS[1] .. ":" .. ids[i]
			if redis.call("exists", key) == 0 then
				redis.call("setex", key, %d, 1)
				retv[#retv + 1] = ids[i]
			end
		end

		return retv
	`, int64(domain.NotificationCheckTimeout.Seconds()))

	return redis.ScriptLoad(ctx, lua).Result()
}

func pruneAccounts(ctx context.Context, logger *logrus.Logger, pool *pgxpool.Pool) {
	expiry := time.Now().Add(-domain.StaleTokenThreshold)
	ar := repository.NewPostgresAccount(pool)

	stale, err := ar.PruneStale(ctx, expiry)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed cleaning stale accounts")
		return
	}

	orphaned, err := ar.PruneOrphaned(ctx)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed cleaning orphaned accounts")
		return
	}

	if count := stale + orphaned; count > 0 {
		logger.WithFields(logrus.Fields{
			"stale":    stale,
			"orphaned": orphaned,
		}).Info("pruned accounts")
	}
}

func pruneDevices(ctx context.Context, logger *logrus.Logger, pool *pgxpool.Pool) {
	now := time.Now()
	dr := repository.NewPostgresDevice(pool)

	count, err := dr.PruneStale(ctx, now)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed cleaning stale devices")
		return
	}

	if count > 0 {
		logger.WithFields(logrus.Fields{
			"count": count,
		}).Info("pruned devices")
	}
}

func cleanQueues(logger *logrus.Logger, jobsConn rmq.Connection) {
	cleaner := rmq.NewCleaner(jobsConn)
	count, err := cleaner.Clean()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed cleaning jobs from queues")
		return
	}

	if count > 0 {
		logger.WithFields(logrus.Fields{
			"count": count,
		}).Info("returned jobs to queues")
	}
}

func reportStats(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool) {
	var (
		count int64

		metrics = []struct {
			query string
			name  string
		}{
			{"SELECT COUNT(*) FROM accounts", "apollo.registrations.accounts"},
			{"SELECT COUNT(*) FROM devices", "apollo.registrations.devices"},
			{"SELECT COUNT(*) FROM subreddits", "apollo.registrations.subreddits"},
			{"SELECT COUNT(*) FROM users", "apollo.registrations.users"},
		}
	)

	for _, metric := range metrics {
		_ = pool.QueryRow(ctx, metric.query).Scan(&count)
		_ = statsd.Gauge(metric.name, float64(count), []string{}, 1)

		logger.WithFields(logrus.Fields{
			"count":  count,
			"metric": metric.name,
		}).Debug("fetched metrics")
	}
}

func enqueueUsers(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, queue rmq.Queue) {
	now := time.Now()
	next := now.Add(domain.NotificationCheckInterval)

	ids := []int64{}

	defer func() {
		tags := []string{"queue:users"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(len(ids)), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE users
			SET next_check_at = $2
			WHERE id IN (
				SELECT id
				FROM users
				WHERE next_check_at < $1
				ORDER BY next_check_at
				FOR UPDATE SKIP LOCKED
				LIMIT 100
			)
			RETURNING users.id`
		rows, err := tx.Query(ctx, stmt, now, next)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			_ = rows.Scan(&id)
			ids = append(ids, id)
		}
		return nil
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch batch of users")
		return
	}

	if len(ids) == 0 {
		return
	}

	logger.WithFields(logrus.Fields{
		"count": len(ids),
		"start": now,
	}).Debug("enqueueing user batch")

	batchIds := make([]string, len(ids))
	for i, id := range ids {
		batchIds[i] = strconv.FormatInt(id, 10)
	}

	if err = queue.Publish(batchIds...); err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to enqueue user")
	}
}

func enqueueSubreddits(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, queues []rmq.Queue) {
	now := time.Now()
	next := now.Add(domain.SubredditCheckInterval)

	ids := []int64{}

	defer func() {
		tags := []string{"queue:subreddits"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(len(ids)), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE subreddits
			SET next_check_at = $2
			WHERE subreddits.id IN(
				SELECT id
				FROM subreddits
				WHERE next_check_at < $1
				ORDER BY next_check_at
				FOR UPDATE SKIP LOCKED
				LIMIT 100
			)
			RETURNING subreddits.id`
		rows, err := tx.Query(ctx, stmt, now, next)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			_ = rows.Scan(&id)
			ids = append(ids, id)
		}
		return nil
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch batch of subreddits")
		return
	}

	if len(ids) == 0 {
		return
	}

	logger.WithFields(logrus.Fields{
		"count": len(ids),
		"start": now,
	}).Debug("enqueueing subreddit batch")

	batchIds := make([]string, len(ids))
	for i, id := range ids {
		batchIds[i] = strconv.FormatInt(id, 10)
	}

	for _, queue := range queues {
		if err = queue.Publish(batchIds...); err != nil {
			logger.WithFields(logrus.Fields{
				"queue": queue,
				"err":   err,
			}).Error("failed to enqueue subreddit")
		}
	}

}

func enqueueStuckAccounts(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, queue rmq.Queue) {
	now := time.Now()
	next := now.Add(domain.StuckNotificationCheckInterval)

	ids := []int64{}

	defer func() {
		tags := []string{"queue:stuck-accounts"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(len(ids)), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE accounts
			SET next_stuck_notification_check_at = $2
			WHERE accounts.id IN(
				SELECT id
				FROM accounts
				WHERE next_stuck_notification_check_at < $1
				ORDER BY next_stuck_notification_check_at
				FOR UPDATE SKIP LOCKED
				LIMIT 500
			)
			RETURNING accounts.id`
		rows, err := tx.Query(ctx, stmt, now, next)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			_ = rows.Scan(&id)
			ids = append(ids, id)
		}
		return nil
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch possible stuck accounts")
		return
	}

	if len(ids) == 0 {
		return
	}

	logger.WithFields(logrus.Fields{
		"count": len(ids),
		"start": now,
	}).Debug("enqueueing stuck account batch")

	batchIds := make([]string, len(ids))
	for i, id := range ids {
		batchIds[i] = strconv.FormatInt(id, 10)
	}

	if err = queue.Publish(batchIds...); err != nil {
		logger.WithFields(logrus.Fields{
			"queue": queue,
			"err":   err,
		}).Error("failed to enqueue stuck accounts")
	}
}

func enqueueAccounts(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client, luaSha string, queue rmq.Queue) {
	now := time.Now()
	next := now.Add(domain.NotificationCheckInterval)

	ids := []int64{}
	enqueued := 0
	skipped := 0

	defer func() {
		tags := []string{"queue:notifications"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(enqueued), tags, 1)
		_ = statsd.Histogram("apollo.queue.skipped", float64(skipped), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE accounts
			SET next_notification_check_at = $2
			WHERE accounts.id IN(
				SELECT id
				FROM accounts
				WHERE next_notification_check_at < $1
				ORDER BY next_notification_check_at
				FOR UPDATE SKIP LOCKED
				LIMIT 5000
			)
			RETURNING accounts.id`
		rows, err := tx.Query(ctx, stmt, now, next)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			_ = rows.Scan(&id)
			ids = append(ids, id)
		}
		return nil
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch batch of accounts")
		return
	}

	if len(ids) == 0 {
		return
	}

	logger.WithFields(logrus.Fields{
		"count": len(ids),
		"start": now,
	}).Debug("enqueueing account batch")
	// Split ids in batches
	for i := 0; i < len(ids); i += batchSize {
		j := i + batchSize
		if j > len(ids) {
			j = len(ids)
		}
		batch := Int64Slice(ids[i:j])

		logger.WithFields(logrus.Fields{
			"len": len(batch),
		}).Debug("enqueueing batch")

		res, err := redisConn.EvalSha(ctx, luaSha, []string{"locks:accounts"}, batch).Result()
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("failed to check for locked accounts")

		}

		vals := res.([]interface{})
		skipped += len(batch) - len(vals)
		enqueued += len(vals)

		if len(vals) == 0 {
			continue
		}

		batchIds := make([]string, len(vals))
		for k, v := range vals {
			batchIds[k] = strconv.FormatInt(v.(int64), 10)
		}

		if err = queue.Publish(batchIds...); err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("failed to enqueue account")
		}
	}

	logger.WithFields(logrus.Fields{
		"count":   enqueued,
		"skipped": skipped,
		"start":   now,
	}).Debug("done enqueueing account batch")
}

type Int64Slice []int64

func (ii Int64Slice) MarshalBinary() (data []byte, err error) {
	bytes, err := json.Marshal(ii)
	return bytes, err
}
