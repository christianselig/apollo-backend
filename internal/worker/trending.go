package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/DataDog/datadog-go/statsd"
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
	logger *zap.Logger
	statsd *statsd.Client
	redis  *redis.Client
	reddit *reddit.Client
	apns   *apns2.Client

	accountRepo   domain.AccountRepository
	deviceRepo    domain.DeviceRepository
	subredditRepo domain.SubredditRepository
	watcherRepo   domain.WatcherRepository
}

const trendingNotificationTitleFormat = "🔥 r/%s Trending"

func NewTrendingWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		redis,
		consumers,
	)

	var apns *apns2.Client
	{
		authKey, err := token.AuthKeyFromFile(os.Getenv("APPLE_KEY_PATH"))
		if err != nil {
			panic(err)
		}

		tok := &token.Token{
			AuthKey: authKey,
			KeyID:   os.Getenv("APPLE_KEY_ID"),
			TeamID:  os.Getenv("APPLE_TEAM_ID"),
		}
		apns = apns2.NewTokenClient(tok).Production()
	}

	return &trendingWorker{
		logger,
		statsd,
		redis,
		reddit,
		apns,

		repository.NewPostgresAccount(db),
		repository.NewPostgresDevice(db),
		repository.NewPostgresSubreddit(db),
		repository.NewPostgresWatcher(db),
	}
}

func (tw *trendingWorker) Process(ctx context.Context, args ...interface{}) error {
	id := int64(args[0].(float64))
	tw.logger.Debug("starting job", zap.Int64("subreddit#id", id))

	subreddit, err := tw.subredditRepo.GetByID(ctx, id)
	if err != nil {
		tw.logger.Error("failed to fetch subreddit from database", zap.Error(err), zap.Int64("subreddit#id", id))
		return nil
	}

	watchers, err := tw.watcherRepo.GetByTrendingSubredditID(ctx, subreddit.ID)
	if err != nil {
		tw.logger.Error("failed to fetch watchers from database",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return err
	}

	if len(watchers) == 0 {
		tw.logger.Debug("no watchers for subreddit, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return nil
	}

	// Grab last month's top posts so we calculate a trending average
	i := rand.Intn(len(watchers))
	watcher := watchers[i]
	rac := tw.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)

	tps, err := rac.SubredditTop(ctx, subreddit.Name, reddit.WithQuery("t", "week"), reddit.WithQuery("show", "all"), reddit.WithQuery("limit", "25"))
	if err != nil {
		tw.logger.Error("failed to fetch weeks's top posts",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return nil
	}

	tw.logger.Debug("loaded weeks's top posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
		zap.Int("count", tps.Count),
	)

	if tps.Count == 0 {
		tw.logger.Debug("no top posts, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return nil
	}

	if tps.Count < 20 {
		tw.logger.Debug("no top posts, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
			zap.Int("count", tps.Count),
		)
		return nil
	}

	sort.SliceStable(tps.Children, func(i, j int) bool {
		return tps.Children[i].Score > tps.Children[j].Score
	})

	middlePost := tps.Count / 2
	medianScore := tps.Children[middlePost].Score
	tw.logger.Debug("calculated median score",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
		zap.Int64("score", medianScore),
	)

	// Grab hot posts and filter out anything that's > 2 days old
	i = rand.Intn(len(watchers))
	watcher = watchers[i]
	rac = tw.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)

	hps, err := rac.SubredditHot(ctx, subreddit.Name, reddit.WithQuery("show", "all"), reddit.WithQuery("always_show_media", "1"))
	if err != nil {
		tw.logger.Error("failed to fetch hot posts",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return err
	}
	tw.logger.Debug("loaded hot posts",
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
			notified, _ := tw.redis.Get(ctx, lockKey).Bool()

			if notified {
				tw.logger.Debug("already notified, skipping",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
					zap.String("post#id", post.ID),
				)
				continue
			}

			tw.redis.SetEX(ctx, lockKey, true, 48*time.Hour)

			if err := tw.watcherRepo.IncrementHits(ctx, watcher.ID); err != nil {
				tw.logger.Error("could not increment hits",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
				)
				return err
			}

			notification.DeviceToken = watcher.Device.APNSToken

			res, err := tw.apns.Push(notification)
			if err != nil {
				_ = tw.statsd.Incr("apns.notification.errors", []string{}, 1)
				tw.logger.Error("failed to send notification",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
				)
			} else if !res.Sent() {
				_ = tw.statsd.Incr("apns.notification.errors", []string{}, 1)
				tw.logger.Error("notification not sent",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)
			} else {
				_ = tw.statsd.Incr("apns.notification.sent", []string{}, 1)
				tw.logger.Info("sent notification",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.Int64("post#score", post.Score),
					zap.String("device#token", watcher.Device.APNSToken),
				)
			}
		}
	}

	tw.logger.Debug("finishing job",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)

	return nil
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
