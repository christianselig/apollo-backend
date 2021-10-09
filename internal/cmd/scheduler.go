package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/christianselig/apollo-backend/internal/cmdutil"
	"github.com/christianselig/apollo-backend/internal/repository"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	batchSize    = 250
	checkTimeout = 60 // how long until we force a check

	accountEnqueueTimeout   = 5      // how frequently we want to check (seconds)
	subredditEnqueueTimeout = 2 * 60 // how frequently we want to check (seconds)
	userEnqueueTimeout      = 2 * 60 // how frequently we want to check (seconds)

	staleAccountThreshold = 7200 // 2 hours
)

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

			userQueue, err := queue.OpenQueue("users")
			if err != nil {
				return err
			}

			s := gocron.NewScheduler(time.UTC)
			_, _ = s.Every(200).Milliseconds().SingletonMode().Do(func() { enqueueAccounts(ctx, logger, statsd, db, redis, luaSha, notifQueue) })
			_, _ = s.Every(200).Milliseconds().SingletonMode().Do(func() { enqueueSubreddits(ctx, logger, statsd, db, subredditQueue) })
			_, _ = s.Every(200).Milliseconds().SingletonMode().Do(func() { enqueueUsers(ctx, logger, statsd, db, userQueue) })
			_, _ = s.Every(1).Second().Do(func() { cleanQueues(ctx, logger, queue) })
			_, _ = s.Every(1).Minute().Do(func() { reportStats(ctx, logger, statsd, db, redis) })
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
	`, checkTimeout)

	return redis.ScriptLoad(ctx, lua).Result()
}

func pruneAccounts(ctx context.Context, logger *logrus.Logger, pool *pgxpool.Pool) {
	before := time.Now().Unix() - staleAccountThreshold
	ar := repository.NewPostgresAccount(pool)

	stale, err := ar.PruneStale(ctx, before)
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

	count := stale + orphaned

	if count > 0 {
		logger.WithFields(logrus.Fields{
			"count": count,
		}).Info("pruned accounts")
	}
}

func pruneDevices(ctx context.Context, logger *logrus.Logger, pool *pgxpool.Pool) {
	threshold := time.Now().Unix()
	dr := repository.NewPostgresDevice(pool)

	count, err := dr.PruneStale(ctx, threshold)
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

func cleanQueues(ctx context.Context, logger *logrus.Logger, jobsConn rmq.Connection) {
	cleaner := rmq.NewCleaner(jobsConn)
	count, err := cleaner.Clean()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed cleaning jobs from queues")
		return
	}

	logger.WithFields(logrus.Fields{
		"count": count,
	}).Debug("returned jobs to queues")
}

func reportStats(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client) {
	var (
		count int64

		metrics = []struct {
			query string
			name  string
		}{
			{"SELECT COUNT(*) FROM accounts", "apollo.registrations.accounts"},
			{"SELECT COUNT(*) FROM devices", "apollo.registrations.devices"},
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
	ready := now.Unix() - userEnqueueTimeout

	ids := []int64{}

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			WITH userb AS (
			  SELECT id
				FROM users
				WHERE last_checked_at < $1
				ORDER BY last_checked_at
				LIMIT 100
			)
			UPDATE users
			SET last_checked_at = $2
			WHERE users.id IN(SELECT id FROM userb)
			RETURNING users.id`
		rows, err := tx.Query(ctx, stmt, ready, now.Unix())
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
		"start": ready,
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

	_ = statsd.Histogram("apollo.queue.users.enqueued", float64(len(ids)), []string{}, 1)
	_ = statsd.Histogram("apollo.queue.users.runtime", float64(time.Since(now).Milliseconds()), []string{}, 1)

}

func enqueueSubreddits(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, queue rmq.Queue) {
	now := time.Now()
	ready := now.Unix() - subredditEnqueueTimeout

	ids := []int64{}

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			WITH subreddit AS (
			  SELECT id
				FROM subreddits
				WHERE last_checked_at < $1
				ORDER BY last_checked_at
				LIMIT 100
			)
			UPDATE subreddits
			SET last_checked_at = $2
			WHERE subreddits.id IN(SELECT id FROM subreddit)
			RETURNING subreddits.id`
		rows, err := tx.Query(ctx, stmt, ready, now.Unix())
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
		"start": ready,
	}).Debug("enqueueing subreddit batch")

	batchIds := make([]string, len(ids))
	for i, id := range ids {
		batchIds[i] = strconv.FormatInt(id, 10)
	}

	if err = queue.Publish(batchIds...); err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to enqueue subreddit")
	}

	_ = statsd.Histogram("apollo.queue.subreddits.enqueued", float64(len(ids)), []string{}, 1)
	_ = statsd.Histogram("apollo.queue.subreddits.runtime", float64(time.Since(now).Milliseconds()), []string{}, 1)

}

func enqueueAccounts(ctx context.Context, logger *logrus.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client, luaSha string, queue rmq.Queue) {
	start := time.Now()

	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	// Start looking for accounts that were last checked at least 5 seconds ago
	// and at most 6 seconds ago. Also look for accounts that haven't been checked
	// in over a minute.
	ts := start.Unix()
	ready := ts - accountEnqueueTimeout
	expired := ts - checkTimeout

	ids := []int64{}

	err := pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			WITH account AS (
			  SELECT id
				FROM accounts
				WHERE
					last_enqueued_at < $1
					OR last_checked_at < $2
				ORDER BY last_checked_at
				LIMIT 2500
			)
			UPDATE accounts
			SET last_enqueued_at = $3
			WHERE accounts.id IN(SELECT id FROM account)
			RETURNING accounts.id`
		rows, err := tx.Query(ctx, stmt, ready, expired, now)
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

	logger.WithFields(logrus.Fields{
		"count": len(ids),
		"start": ready,
	}).Debug("enqueueing account batch")

	enqueued := 0
	skipped := 0

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

	_ = statsd.Histogram("apollo.queue.notifications.enqueued", float64(enqueued), []string{}, 1)
	_ = statsd.Histogram("apollo.queue.notifications.skipped", float64(skipped), []string{}, 1)
	_ = statsd.Histogram("apollo.queue.notifications.runtime", float64(time.Since(start).Milliseconds()), []string{}, 1)

	logger.WithFields(logrus.Fields{
		"count":   enqueued,
		"skipped": skipped,
		"start":   ready,
	}).Debug("done enqueueing account batch")
}

type Int64Slice []int64

func (ii Int64Slice) MarshalBinary() (data []byte, err error) {
	bytes, err := json.Marshal(ii)
	return bytes, err
}
