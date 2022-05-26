package worker

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
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

type subredditsWorker struct {
	context.Context

	logger *zap.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
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

const (
	subredditNotificationTitleFormat = "📣 \u201c%s\u201d Watcher"
	subredditNotificationBodyFormat  = "r/%s: \u201c%s\u201d"
)

func NewSubredditsWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
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

	return &subredditsWorker{
		ctx,
		logger,
		statsd,
		db,
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

func (sw *subredditsWorker) Start() error {
	queue, err := sw.queue.OpenQueue("subreddits")
	if err != nil {
		return err
	}

	sw.logger.Info("starting up subreddits worker", zap.Int("consumers", sw.consumers))

	prefetchLimit := int64(sw.consumers * 2)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < sw.consumers; i++ {
		name := fmt.Sprintf("consumer %s-%d", host, i)

		consumer := NewSubredditsConsumer(sw, i)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			return err
		}
	}

	return nil
}

func (sw *subredditsWorker) Stop() {
	<-sw.queue.StopAllConsuming() // wait for all Consume() calls to finish
}

type subredditsConsumer struct {
	*subredditsWorker
	tag int

	apnsSandbox    *apns2.Client
	apnsProduction *apns2.Client
}

func NewSubredditsConsumer(sw *subredditsWorker, tag int) *subredditsConsumer {
	return &subredditsConsumer{
		sw,
		tag,
		apns2.NewTokenClient(sw.apns),
		apns2.NewTokenClient(sw.apns).Production(),
	}
}

func (sc *subredditsConsumer) Consume(delivery rmq.Delivery) {
	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		sc.logger.Error("failed to parse subreddit id from payload", zap.Error(err), zap.String("payload", delivery.Payload()))
		_ = delivery.Reject()
		return
	}

	sc.logger.Debug("starting job", zap.Int64("subreddit#id", id))

	defer func() { _ = delivery.Ack() }()

	subreddit, err := sc.subredditRepo.GetByID(sc, id)
	if err != nil {
		sc.logger.Error("failed to fetch subreddit from database", zap.Error(err), zap.Int64("subreddit#id", id))
		return
	}

	watchers, err := sc.watcherRepo.GetBySubredditID(sc, subreddit.ID)
	if err != nil {
		sc.logger.Error("failed to fetch watchers from database",
			zap.Error(err),
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}

	if len(watchers) == 0 {
		sc.logger.Debug("no watchers for subreddit, bailing early",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
		)
		return
	}

	threshold := time.Now().Add(-24 * time.Hour)
	posts := []*reddit.Thing{}
	before := ""
	finished := false
	seenPosts := map[string]bool{}

	// Load 500 newest posts
	sc.logger.Debug("loading up to 500 new posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)

	for page := 0; page < 5; page++ {
		sc.logger.Debug("loading new posts",
			zap.Int64("subreddit#id", id),
			zap.String("subreddit#name", subreddit.NormalizedName()),
			zap.Int("page", page),
		)

		sps, err := sc.reddit.SubredditNew(sc,
			subreddit.Name,
			reddit.WithQuery("before", before),
			reddit.WithQuery("limit", "100"),
		)

		if err != nil {
			sc.logger.Error("failed to fetch new posts",
				zap.Error(err),
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int("page", page),
			)
			continue
		}

		sc.logger.Debug("loaded new posts",
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
			sc.logger.Debug("reached date threshold",
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int("page", page),
			)
			break
		}
	}

	// Load hot posts
	sc.logger.Debug("loading hot posts",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)
	{
		sps, err := sc.reddit.SubredditHot(sc,
			subreddit.Name,
			reddit.WithQuery("limit", "100"),
		)

		if err != nil {
			sc.logger.Error("failed to fetch hot posts",
				zap.Error(err),
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
			)
		} else {
			sc.logger.Debug("loaded hot posts",
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

	sc.logger.Debug("checking posts for watcher hits",
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

			sc.logger.Info("matched post",
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
			notified, _ := sc.redis.Get(sc, lockKey).Bool()

			if notified {
				sc.logger.Debug("already notified, skipping",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
					zap.String("post#id", post.ID),
				)
				continue
			}

			if err := sc.watcherRepo.IncrementHits(sc, watcher.ID); err != nil {
				sc.logger.Error("could not increment hits",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
				)
				return
			}
			sc.logger.Debug("got a hit",
				zap.Int64("subreddit#id", id),
				zap.String("subreddit#name", subreddit.NormalizedName()),
				zap.Int64("watcher#id", watcher.ID),
				zap.String("post#id", post.ID),
			)

			sc.redis.SetEX(sc, lockKey, true, 24*time.Hour)
			notifs = append(notifs, watcher)
		}

		if len(notifs) == 0 {
			continue
		}
		sc.logger.Debug("got hits for post",
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

			client := sc.apnsProduction
			if watcher.Device.Sandbox {
				client = sc.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil || !res.Sent() {
				_ = sc.statsd.Incr("apns.notification.errors", []string{}, 1)
				sc.logger.Error("failed to send notification",
					zap.Error(err),
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)
			} else {
				_ = sc.statsd.Incr("apns.notification.sent", []string{}, 1)
				sc.logger.Info("sent notification",
					zap.Int64("subreddit#id", id),
					zap.String("subreddit#name", subreddit.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("device#token", watcher.Device.APNSToken),
				)
			}
		}
	}

	sc.logger.Debug("finishing job",
		zap.Int64("subreddit#id", id),
		zap.String("subreddit#name", subreddit.NormalizedName()),
	)
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

	if post.Thumbnail != "" {
		payload.Custom("thumbnail", post.Thumbnail)
	}

	return payload
}
