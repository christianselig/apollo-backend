package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type trendingWorker struct {
	context.Context

	logger *zap.Logger
	statsd *statsd.Client
	redis  *redis.Client
	queue  rmq.Connection
	reddit *reddit.Client
	apns   *token.Token

	consumers int

	accountRepo   domain.AccountRepository
	deviceRepo    domain.DeviceRepository
	subredditRepo domain.SubredditRepository
	watcherRepo   domain.WatcherRepository
}

const trendingNotificationTitleFormat = "🔥 r/%s Trending"

func NewTrendingWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		redis,
		consumers,
	)

	var apns *token.Token
	{
		authKey, err := token.AuthKeyFromFile(os.Getenv("APPLE_KEY_PATH"))
		if err != nil {
			panic(err)
		}

		apns = &token.Token{
			AuthKey: authKey,
			KeyID:   os.Getenv("APPLE_KEY_ID"),
			TeamID:  os.Getenv("APPLE_TEAM_ID"),
		}
	}

	return &trendingWorker{
		ctx,
		logger,
		statsd,
		redis,
		queue,
		reddit,
		apns,
		consumers,

		repository.NewPostgresAccount(db),
		repository.NewPostgresDevice(db),
		repository.NewPostgresSubreddit(db),
		repository.NewPostgresWatcher(db),
	}
}

func (tw *trendingWorker) Start() error {
	queue, err := tw.queue.OpenQueue("trending")
	if err != nil {
		return err
	}

	tw.logger.Info("starting up trending subreddits worker", zap.Int("consumers", tw.consumers))

	prefetchLimit := int64(tw.consumers * 2)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < tw.consumers; i++ {
		name := fmt.Sprintf("consumer %s-%d", host, i)

		consumer := NewTrendingConsumer(tw, i)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			return err
		}
	}

	return nil
}

func (tw *trendingWorker) Stop() {
	<-tw.queue.StopAllConsuming() // wait for all Consume() calls to finish
}

type trendingConsumer struct {
	*trendingWorker
	tag int

	apnsSandbox    *apns2.Client
	apnsProduction *apns2.Client
}

func NewTrendingConsumer(tw *trendingWorker, tag int) *trendingConsumer {
	return &trendingConsumer{
		tw,
		tag,
		apns2.NewTokenClient(tw.apns),
		apns2.NewTokenClient(tw.apns).Production(),
	}
}

func (tc *trendingConsumer) Consume(delivery rmq.Delivery) {
	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		tc.logger.Error("failed to parse subreddit id from payload", zap.Error(err), zap.String("payload", delivery.Payload()))
		_ = delivery.Reject()
		return
	}

	tc.logger.Debug("starting job", zap.Int64("subreddit#id", id))

	defer func() { _ = delivery.Ack() }()

	subreddit, err := tc.subredditRepo.GetByID(tc, id)
	if err != nil {
		tc.logger.Error("failed to fetch subreddit from database", zap.Error(err), zap.Int64("subreddit#id", id))
		return
	}

	watchers, err := tc.watcherRepo.GetByTrendingSubredditID(tc, subreddit.ID)
	if err != nil {
		tc.logger.Error("failed to fetch watchers from database",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}

	if len(watchers) == 0 {
		tc.logger.Debug("no watchers for subreddit, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}

	// Grab last month's top posts so we calculate a trending average
	i := rand.Intn(len(watchers))
	watcher := watchers[i]
	rac := tc.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)

	tps, err := rac.SubredditTop(tc, subreddit.Name, reddit.WithQuery("t", "week"), reddit.WithQuery("show", "all"), reddit.WithQuery("limit", "25"))
	if err != nil {
		tc.logger.Error("failed to fetch weeks's top posts",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}

	tc.logger.Debug("loaded weeks's top posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
		zap.Int("count", tps.Count),
	)

	if tps.Count == 0 {
		tc.logger.Debug("no top posts, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}

	if tps.Count < 20 {
		tc.logger.Debug("no top posts, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
			zap.Int("count", tps.Count),
		)
		return
	}

	sort.SliceStable(tps.Children, func(i, j int) bool {
		return tps.Children[i].Score > tps.Children[j].Score
	})

	middlePost := tps.Count / 2
	medianScore := tps.Children[middlePost].Score
	tc.logger.Debug("calculated median score",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
		zap.Int64("score", medianScore),
	)

	// Grab hot posts and filter out anything that's > 2 days old
	i = rand.Intn(len(watchers))
	watcher = watchers[i]
	rac = tc.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)
	hps, err := tc.reddit.SubredditHot(tc, subreddit.Name, reddit.WithQuery("show", "all"))
	if err != nil {
		tc.logger.Error("failed to fetch hot posts",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}
	tc.logger.Debug("loaded hot posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
		zap.Int("count", hps.Count),
	)

	// Trending only counts for posts less than 2 days old
	threshold := time.Now().Add(-24 * time.Hour * 2)

	for _, post := range hps.Children {
		if post.Score < medianScore {
			continue
		}

		if post.CreatedAt.Before(threshold) {
			break
		}

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromTrendingPost(post)

		for _, watcher := range watchers {
			if watcher.CreatedAt.After(post.CreatedAt) {
				continue
			}

			lockKey := fmt.Sprintf("watcher:trending:%d:%s", watcher.DeviceID, post.ID)
			notified, _ := tc.redis.Get(tc, lockKey).Bool()

			if notified {
				tc.logger.Debug("already notified, skipping",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
					zap.String("post#id", post.ID),
				)
				continue
			}

			tc.redis.SetEX(tc, lockKey, true, 48*time.Hour)

			if err := tc.watcherRepo.IncrementHits(tc, watcher.ID); err != nil {
				tc.logger.Error("could not increment hits",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
				)
				return
			}

			notification.DeviceToken = watcher.Device.APNSToken

			client := tc.apnsProduction
			if watcher.Device.Sandbox {
				client = tc.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil || !res.Sent() {
				_ = tc.statsd.Incr("apns.notification.errors", []string{}, 1)
				tc.logger.Error("failed to send notification",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)
			} else {
				_ = tc.statsd.Incr("apns.notification.sent", []string{}, 1)
				tc.logger.Info("sent notification",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.Int64("post#score", post.Score),
					zap.String("device#token", watcher.Device.APNSToken),
				)
			}
		}
	}

	tc.logger.Debug("finishing job",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)
}

func payloadFromTrendingPost(post *reddit.Thing) *payload.Payload {
	title := fmt.Sprintf(trendingNotificationTitleFormat, post.Subreddit)

	payload := payload.
		NewPayload().
		AlertTitle(title).
		AlertBody(post.Title).
		AlertSummaryArg(post.Subreddit).
		Category("trending-post").
		Custom("post_title", post.Title).
		Custom("post_id", post.ID).
		Custom("subreddit", post.Subreddit).
		Custom("author", post.Author).
		Custom("post_age", post.CreatedAt).
		ThreadID("trending-post").
		MutableContent().
		Sound("traloop.wav")

	if post.Thumbnail != "" && !post.Over18 {
		payload.Custom("thumbnail", post.Thumbnail)
	}

	return payload
}
