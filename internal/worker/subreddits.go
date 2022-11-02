package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
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

type subredditsWorker struct {
	logger *zap.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
	redis  *redis.Client
	reddit *reddit.Client
	apns   *apns2.Client

	accountRepo   domain.AccountRepository
	deviceRepo    domain.DeviceRepository
	subredditRepo domain.SubredditRepository
	watcherRepo   domain.WatcherRepository
}

const (
	subredditNotificationTitleFormat = "📣 \u201c%s\u201d Watcher"
	subredditNotificationBodyFormat  = "r/%s: \u201c%s\u201d"
)

func NewSubredditsWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, consumers int) Worker {
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

	return &subredditsWorker{
		logger,
		statsd,
		db,
		redis,
		reddit,
		apns,
		repository.NewPostgresAccount(db),
		repository.NewPostgresDevice(db),
		repository.NewPostgresSubreddit(db),
		repository.NewPostgresWatcher(db),
	}
}

func (sw *subredditsWorker) Process(ctx context.Context, args ...interface{}) error {
	id := int64(args[0].(float64))
	sw.logger.Debug("starting job", zap.Int64("subreddit#id", id))

	subreddit, err := sw.subredditRepo.GetByID(ctx, id)
	if err != nil {
		sw.logger.Error("failed to fetch subreddit from database", zap.Error(err), zap.Int64("subreddit#id", id))
		return nil
	}

	watchers, err := sw.watcherRepo.GetBySubredditID(ctx, subreddit.ID)
	if err != nil {
		sw.logger.Error("failed to fetch watchers from database",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return err
	}

	if len(watchers) == 0 {
		sw.logger.Debug("no watchers for subreddit, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return nil
	}

	threshold := time.Now().Add(-24 * time.Hour)
	posts := []*reddit.Thing{}
	before := ""
	finished := false
	seenPosts := map[string]bool{}

	// Load 500 newest posts
	sw.logger.Debug("loading up to 500 new posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)

	for page := 0; page < 5; page++ {
		sw.logger.Debug("loading new posts",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
			zap.Int("page", page),
		)

		i := rand.Intn(len(watchers))
		watcher := watchers[i]

		rac := sw.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)
		sps, err := rac.SubredditNew(ctx,
			subreddit.Name,
			reddit.WithQuery("before", before),
			reddit.WithQuery("limit", "100"),
			reddit.WithQuery("show", "all"),
			reddit.WithQuery("always_show_media", "1"),
		)

		if err != nil {
			sw.logger.Error("failed to fetch new posts",
				zap.Error(err),
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int("page", page),
			)
			continue
		}

		sw.logger.Debug("loaded new posts",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
			zap.Int("page", page),
			zap.Int("count", sps.Count),
		)

		// If it's empty, we're done
		if sps.Count == 0 {
			break
		}

		// If we don't have 100 posts, we're going to be done
		if sps.Count < 100 {
			finished = true
		}

		for _, post := range sps.Children {
			if post.CreatedAt.Before(threshold) {
				finished = true
				break
			}

			if _, ok := seenPosts[post.ID]; !ok {
				posts = append(posts, post)
				seenPosts[post.ID] = true
			}
		}

		if finished {
			sw.logger.Debug("reached date threshold",
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int("page", page),
			)
			break
		}
	}

	// Load hot posts
	sw.logger.Debug("loading hot posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)
	{
		i := rand.Intn(len(watchers))
		watcher := watchers[i]

		rac := sw.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)
		sps, err := rac.SubredditHot(ctx,
			subreddit.Name,
			reddit.WithQuery("limit", "100"),
			reddit.WithQuery("show", "all"),
			reddit.WithQuery("always_show_media", "1"),
		)

		if err != nil {
			sw.logger.Error("failed to fetch hot posts",
				zap.Error(err),
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
			)
		} else {
			sw.logger.Debug("loaded hot posts",
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int("count", sps.Count),
			)

			for _, post := range sps.Children {
				if post.CreatedAt.Before(threshold) {
					break
				}
				if _, ok := seenPosts[post.ID]; !ok {
					posts = append(posts, post)
					seenPosts[post.ID] = true
				}
			}
		}
	}

	sw.logger.Debug("checking posts for watcher hits",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
		zap.Int("count", len(posts)),
	)
	for _, post := range posts {
		lowcaseAuthor := strings.ToLower(post.Author)
		lowcaseTitle := strings.ToLower(post.Title)
		lowcaseFlair := strings.ToLower(post.Flair)
		lowcaseDomain := strings.ToLower(post.URL)

		notifs := []domain.Watcher{}

		for _, watcher := range watchers {
			// Make sure we only alert on posts created after the search
			if watcher.CreatedAt.After(post.CreatedAt) {
				continue
			}

			matched := watcher.KeywordMatches(lowcaseTitle)

			if watcher.Author != "" && lowcaseAuthor != watcher.Author {
				matched = false
			}

			if watcher.Upvotes > 0 && post.Score < watcher.Upvotes {
				matched = false
			}

			if watcher.Flair != "" && !strings.Contains(lowcaseFlair, watcher.Flair) {
				matched = false
			}

			if watcher.Domain != "" && !strings.Contains(lowcaseDomain, watcher.Domain) {
				matched = false
			}

			if !matched {
				continue
			}

			sw.logger.Debug("matched post",
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int64("watcher#id", watcher.ID),
				zap.String("watcher#keywords", watcher.Keyword),
				zap.Int64("watcher#upvotes", watcher.Upvotes),
				zap.String("post#id", post.ID),
				zap.String("post#title", post.Title),
				zap.Int64("post#score", post.Score),
			)

			lockKey := fmt.Sprintf("watcher:%d:%s", watcher.DeviceID, post.ID)
			notified, _ := sw.redis.Get(ctx, lockKey).Bool()

			if notified {
				sw.logger.Debug("already notified, skipping",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
					zap.String("post#id", post.ID),
				)
				continue
			}

			if err := sw.watcherRepo.IncrementHits(ctx, watcher.ID); err != nil {
				sw.logger.Error("could not increment hits",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
				)
				return err
			}
			sw.logger.Debug("got a hit",
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int64("watcher#id", watcher.ID),
				zap.String("post#id", post.ID),
			)

			sw.redis.SetEX(ctx, lockKey, true, 24*time.Hour)
			notifs = append(notifs, watcher)
		}

		if len(notifs) == 0 {
			continue
		}
		sw.logger.Debug("got hits for post",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
			zap.String("post#id", post.ID),
			zap.Int("count", len(notifs)),
		)

		payload := payloadFromPost(post)

		for _, watcher := range notifs {
			title := fmt.Sprintf(subredditNotificationTitleFormat, watcher.Label)
			payload.AlertTitle(title)

			body := fmt.Sprintf(subredditNotificationBodyFormat, subreddit.Name, post.Title)
			payload.AlertBody(body)

			notification := &apns2.Notification{}
			notification.Topic = "com.christianselig.Apollo"
			notification.DeviceToken = watcher.Device.APNSToken
			notification.Payload = payload

			res, err := sw.apns.Push(notification)
			if err != nil {
				_ = sw.statsd.Incr("apns.notification.errors", []string{}, 1)
				sw.logger.Error("failed to send notification",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
				)
				return err
			} else if !res.Sent() {
				_ = sw.statsd.Incr("apns.notification.errors", []string{}, 1)
				sw.logger.Error("notificaion not sent",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)
			} else {
				_ = sw.statsd.Incr("apns.notification.sent", []string{}, 1)
				sw.logger.Info("sent notification",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("device#token", watcher.Device.APNSToken),
				)
			}
		}
	}

	sw.logger.Debug("finishing job",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)

	return nil
}

func payloadFromPost(post *reddit.Thing) *payload.Payload {
	payload := payload.
		NewPayload().
		AlertSummaryArg(post.Subreddit).
		Category("subreddit-watcher").
		Custom("post_title", post.Title).
		Custom("post_id", post.ID).
		Custom("subreddit", post.Subreddit).
		Custom("author", post.Author).
		Custom("post_age", post.CreatedAt).
		ThreadID("subreddit-watcher").
		MutableContent().
		Sound("traloop.wav")

	if post.Thumbnail != "" && !post.Over18 {
		payload.Custom("thumbnail", post.Thumbnail)
	}

	return payload
}
