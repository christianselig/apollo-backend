package cmd

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	faktory "github.com/contribsys/faktory/client"
	"github.com/go-co-op/gocron"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/cmdutil"
	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/repository"
)

const batchSize = 500

func SchedulerCmd(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scheduler",
		Args:  cobra.ExactArgs(0),
		Short: "Schedules jobs and runs several maintenance tasks periodically.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := cmdutil.NewLogger("scheduler")
			defer func() { _ = logger.Sync() }()

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

			redis, err := cmdutil.NewRedisClient(ctx, 16)
			if err != nil {
				return err
			}
			defer redis.Close()

			fp, err := cmdutil.NewFaktoryPool(8)
			if err != nil {
				return err
			}

			// Eval lua so that we don't keep parsing it
			luaSha, err := evalScript(ctx, redis)
			if err != nil {
				return err
			}

			s := gocron.NewScheduler(time.UTC)
			s.SetMaxConcurrentJobs(8, gocron.WaitMode)

			eaj, _ := s.Every(5).Seconds().Do(func() { enqueueAccounts(ctx, logger, statsd, db, redis, luaSha, fp) })
			eaj.SingletonMode()

			_, _ = s.Every(5).Seconds().Do(func() { enqueueSubreddits(ctx, logger, statsd, db, fp) })
			_, _ = s.Every(5).Seconds().Do(func() { enqueueLiveActivities(ctx, logger, db, redis, luaSha, fp) })
			_, _ = s.Every(5).Seconds().Do(func() { enqueueStuckAccounts(ctx, logger, statsd, db, fp) })
			_, _ = s.Every(1).Minute().Do(func() { reportStats(ctx, logger, statsd, db) })
			//_, _ = s.Every(1).Minute().Do(func() { pruneAccounts(ctx, logger, db) })
			//_, _ = s.Every(1).Minute().Do(func() { pruneDevices(ctx, logger, db) })
			s.StartAsync()

			srv := &http.Server{Addr: ":8080"}
			go func() { _ = srv.ListenAndServe() }()

			<-ctx.Done()

			s.Stop()

			return nil
		},
	}

	return cmd
}

func evalScript(ctx context.Context, redis *redis.Client) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lua := fmt.Sprintf(`
		local retv={}

		for i=1, #ARGV do
			local key = KEYS[1] .. ":" .. ARGV[i]
			if redis.call("exists", key) == 0 then
				redis.call("setex", key, %.0f, 1)
				retv[#retv + 1] = ARGV[i]
			end
		end

		return retv
	`, domain.NotificationCheckTimeout.Seconds())

	return redis.ScriptLoad(ctx, lua).Result()
}

func enqueueLiveActivities(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool, redisConn *redis.Client, luaSha string, fp *faktory.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()
	next := now.Add(domain.LiveActivityCheckInterval)

	stmt := `UPDATE live_activities
		SET next_check_at = $2
		WHERE id IN (
			SELECT id
			FROM live_activities
			WHERE next_check_at < $1
			ORDER BY next_check_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1000
		)
		RETURNING live_activities.apns_token`

	ats := []string{}

	rows, err := pool.Query(ctx, stmt, now, next)
	if err != nil {
		logger.Error("failed to fetch batch of live activities", zap.Error(err))
		return
	}
	for rows.Next() {
		var at string
		_ = rows.Scan(&at)
		ats = append(ats, at)
	}
	rows.Close()

	if len(ats) == 0 {
		return
	}

	batch, err := redisConn.EvalSha(ctx, luaSha, []string{"locks:live-activities"}, ats).StringSlice()
	if err != nil {
		logger.Error("failed to lock live activities", zap.Error(err))
		return
	}

	if len(batch) == 0 {
		return
	}

	logger.Debug("enqueueing live activity batch", zap.Int("count", len(batch)), zap.Time("start", now))

	jobs := make([]*faktory.Job, len(batch))
	for i, tok := range batch {
		jobs[i] = faktory.NewJob("LiveActivityJob", tok)
	}

	fc, err := fp.Get()
	if err != nil {
		logger.Error("failed to get faktory client", zap.Error(err))
		return
	}
	defer fp.Put(fc)

	if _, err = fc.PushBulk(jobs); err != nil {
		logger.Error("failed to enqueue live activity batch", zap.Error(err))
	}
}

func pruneAccounts(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	expiry := time.Now().Add(-domain.StaleTokenThreshold)
	ar := repository.NewPostgresAccount(pool)

	stale, err := ar.PruneStale(ctx, expiry)
	if err != nil {
		logger.Error("failed to clean stale accounts", zap.Error(err))
		return
	}

	orphaned, err := ar.PruneOrphaned(ctx)
	if err != nil {
		logger.Error("failed to clean orphaned accounts", zap.Error(err))
		return
	}

	if count := stale + orphaned; count > 0 {
		logger.Info("pruned accounts", zap.Int64("stale", stale), zap.Int64("orphaned", orphaned))
	}
}

func pruneDevices(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()
	dr := repository.NewPostgresDevice(pool)

	count, err := dr.PruneStale(ctx, now)
	if err != nil {
		logger.Error("failed to clean stale devices", zap.Error(err))
		return
	}

	if count > 0 {
		logger.Info("pruned devices", zap.Int64("count", count))
	}
}

func reportStats(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, pool *pgxpool.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
			{"SELECT COUNT(*) FROM live_activities", "apollo.registrations.live-activities"},
		}
	)

	for _, metric := range metrics {
		_ = pool.QueryRow(ctx, metric.query).Scan(&count)
		_ = statsd.Gauge(metric.name, float64(count), []string{}, 1)

		logger.Debug("fetched metrics", zap.String("metric", metric.name), zap.Int64("count", count))
	}
}

func enqueueSubreddits(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, pool *pgxpool.Pool, fp *faktory.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()
	next := now.Add(domain.SubredditCheckInterval)

	ids := []int64{}

	defer func() {
		tags := []string{"queue:subreddits"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(len(ids)), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

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
	rows, err := pool.Query(ctx, stmt, now, next)
	if err != nil {
		logger.Error("failed to fetch batch of subreddits", zap.Error(err))
		return
	}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) == 0 {
		return
	}

	logger.Debug("enqueueing subreddit batch", zap.Int("count", len(ids)), zap.Time("start", now))

	batchIds := make([]string, len(ids))
	for i, id := range ids {
		batchIds[i] = strconv.FormatInt(id, 10)
	}

	jobs := []*faktory.Job{}
	for _, id := range ids {
		jobs = append(jobs, faktory.NewJob("SubredditWatcherJob", id))
		jobs = append(jobs, faktory.NewJob("SubredditTrendingJob", id))
	}

	fc, err := fp.Get()
	if err != nil {
		logger.Error("failed to get faktory client", zap.Error(err))
		return
	}
	defer fp.Put(fc)

	if _, err := fc.PushBulk(jobs); err != nil {
		logger.Error("failed to enqueue subreddit batch", zap.Error(err))
	}
}

func enqueueStuckAccounts(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, pool *pgxpool.Pool, fp *faktory.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()
	next := now.Add(domain.StuckNotificationCheckInterval)

	ids := []string{}

	defer func() {
		tags := []string{"queue:stuck-accounts"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(len(ids)), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

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
			RETURNING accounts.reddit_account_id`
	rows, err := pool.Query(ctx, stmt, now, next)
	if err != nil {
		logger.Error("failed to fetch accounts", zap.Error(err))
		return
	}

	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) == 0 {
		return
	}

	logger.Debug("enqueueing stuck account batch", zap.Int("count", len(ids)), zap.Time("start", now))

	jobs := make([]*faktory.Job, len(ids))
	for i, id := range ids {
		jobs[i] = faktory.NewJob("StuckNotificationsJob", id)
	}

	fc, err := fp.Get()
	if err != nil {
		logger.Error("failed to get faktory client", zap.Error(err))
		return
	}
	defer fp.Put(fc)

	if _, err = fc.PushBulk(jobs); err != nil {
		logger.Error("failed to enqueue stuck account batch", zap.Error(err))
	}
}

func enqueueAccounts(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, pool *pgxpool.Pool, redisConn *redis.Client, luaSha string, fp *faktory.Pool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()

	query := `
		SELECT DISTINCT reddit_account_id FROM accounts
		INNER JOIN devices_accounts ON devices_accounts.account_id = accounts.id
		INNER JOIN devices ON devices.id = devices_accounts.device_id
		WHERE grace_period_expires_at >= NOW()
		AND accounts.is_deleted IS FALSE
	`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		logger.Error("failed to fetch accounts", zap.Error(err))
		return
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}

	var (
		enqueued int64 = 0
		skipped  int64 = 0
	)

	defer func() {
		tags := []string{"queue:notifications"}
		_ = statsd.Histogram("apollo.queue.enqueued", float64(enqueued), tags, 1)
		_ = statsd.Histogram("apollo.queue.skipped", float64(skipped), tags, 1)
		_ = statsd.Histogram("apollo.queue.runtime", float64(time.Since(now).Milliseconds()), tags, 1)
	}()

	logger.Debug("enqueueing account batch", zap.Int("count", len(ids)), zap.Time("start", now))

	// Split ids in batches
	wg := sync.WaitGroup{}
	for i := 0; i < len(ids); i += batchSize {
		wg.Add(1)
		go func(offset int, ctx context.Context) {
			defer wg.Done()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			j := offset + batchSize
			if j > len(ids) {
				j = len(ids)
			}
			batch := ids[offset:j]

			logger.Debug("enqueueing batch", zap.Int("len", len(batch)))

			unlocked, err := redisConn.EvalSha(ctx, luaSha, []string{"locks:accounts"}, batch).StringSlice()
			if err != nil {
				logger.Error("failed to check for locked accounts", zap.Error(err))
			}

			atomic.AddInt64(&skipped, int64(len(batch)-len(unlocked)))
			atomic.AddInt64(&enqueued, int64(len(unlocked)))

			if len(unlocked) == 0 {
				return
			}

			jobs := make([]*faktory.Job, len(ids))
			for i, id := range unlocked {
				jobs[i] = faktory.NewJob("NotificationCheckJob", id)
			}

			fc, err := fp.Get()
			if err != nil {
				logger.Error("failed to get faktory client", zap.Error(err))
				return
			}
			defer fp.Put(fc)

			if _, err = fc.PushBulk(jobs); err != nil {
				logger.Error("failed to enqueue stuck account batch", zap.Error(err))
			}
		}(i, ctx)
	}
	wg.Wait()

	logger.Info("enqueued account batch",
		zap.Int64("count", enqueued),
		zap.Int64("skipped", skipped),
		zap.Int64("duration", time.Since(now).Milliseconds()),
	)
}
