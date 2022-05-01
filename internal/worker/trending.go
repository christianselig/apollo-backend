package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type trendingWorker struct {
	logger *logrus.Logger
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

func NewTrendingWorker(logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
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

	tw.logger.WithFields(logrus.Fields{
		"numConsumers": tw.consumers,
	}).Info("starting up trending worker")

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
	ctx := context.Background()

	tc.logger.WithFields(logrus.Fields{
		"subreddit#id": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id": delivery.Payload(),
			"err":          err,
		}).Error("failed to parse subreddit ID")

		_ = delivery.Reject()
		return
	}

	defer func() { _ = delivery.Ack() }()

	subreddit, err := tc.subredditRepo.GetByID(ctx, id)
	if err != nil {
		tc.logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch subreddit from database")
		return
	}

	watchers, err := tc.watcherRepo.GetByTrendingSubredditID(ctx, subreddit.ID)
	if err != nil {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id": subreddit.ID,
			"err":          err,
		}).Error("failed to fetch watchers from database")
		return
	}

	if len(watchers) == 0 {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id": subreddit.ID,
		}).Debug("no watchers for trending, finishing job")
		return
	}

	// Grab last month's top posts so we calculate a trending average
	i := rand.Intn(len(watchers))
	watcher := watchers[i]
	rac := tc.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)

	tps, err := rac.SubredditTop(subreddit.Name, reddit.WithQuery("t", "week"))
	if err != nil {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id":   subreddit.ID,
			"subreddit#name": subreddit.Name,
			"err":            err,
		}).Error("failed to fetch month's top posts")
		return
	}
	tc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
		"count":          tps.Count,
	}).Debug("loaded month's hot posts")

	if tps.Count == 0 {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id": subreddit.ID,
		}).Debug("no top posts for subreddit, returning")
		return
	}

	if tps.Count < 20 {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id": subreddit.ID,
		}).Debug("not enough posts, returning")
		return
	}

	middlePost := tps.Count / 2
	medianScore := tps.Children[middlePost].Score
	tc.logger.WithFields(logrus.Fields{
		"subreddit#id": subreddit.ID,
		"score":        medianScore,
	}).Debug("calculated median score")

	// Grab hot posts and filter out anything that's > 2 days old
	i = rand.Intn(len(watchers))
	watcher = watchers[i]
	rac = tc.reddit.NewAuthenticatedClient(watcher.Account.AccountID, watcher.Account.RefreshToken, watcher.Account.AccessToken)
	hps, err := rac.SubredditHot(subreddit.Name)
	if err != nil {
		tc.logger.WithFields(logrus.Fields{
			"subreddit#id":   subreddit.ID,
			"subreddit#name": subreddit.Name,
			"err":            err,
		}).Error("failed to fetch hot posts")
		return
	}
	tc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
		"count":          hps.Count,
	}).Debug("loaded hot posts")

	// Trending only counts for posts less than 2 days old
	threshold := float64(time.Now().AddDate(0, 0, -2).UTC().Unix())

	for _, post := range hps.Children {
		if post.Score < medianScore {
			continue
		}

		if post.CreatedAt < threshold {
			break
		}

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromTrendingPost(post)

		for _, watcher := range watchers {
			if watcher.CreatedAt > post.CreatedAt {
				continue
			}

			lockKey := fmt.Sprintf("watcher:trending:%d:%s", watcher.DeviceID, post.ID)
			notified, _ := tc.redis.Get(ctx, lockKey).Bool()

			if notified {
				tc.logger.WithFields(logrus.Fields{
					"subreddit#id":   subreddit.ID,
					"subreddit#name": subreddit.Name,
					"watcher#id":     watcher.ID,
					"post#id":        post.ID,
				}).Debug("already notified, skipping")
				continue
			}

			tc.redis.SetEX(ctx, lockKey, true, 48*time.Hour)

			if err := tc.watcherRepo.IncrementHits(ctx, watcher.ID); err != nil {
				tc.logger.WithFields(logrus.Fields{
					"subreddit#id": subreddit.ID,
					"watcher#id":   watcher.ID,
					"err":          err,
				}).Error("could not increment hits")
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
				tc.logger.WithFields(logrus.Fields{
					"subreddit#id": subreddit.ID,
					"post#id":      post.ID,
					"device#id":    watcher.Device.ID,
					"err":          err,
					"status":       res.StatusCode,
					"reason":       res.Reason,
				}).Error("failed to send notification")
			} else {
				_ = tc.statsd.Incr("apns.notification.sent", []string{}, 1)
				tc.logger.WithFields(logrus.Fields{
					"subreddit#id": subreddit.ID,
					"post#id":      post.ID,
					"device#id":    watcher.Device.ID,
					"device#token": watcher.Device.APNSToken,
				}).Info("sent notification")
			}
		}
	}

	tc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
	}).Debug("finishing job")
}

func payloadFromTrendingPost(post *reddit.Thing) *payload.Payload {
	title := fmt.Sprintf(trendingNotificationTitleFormat, post.Subreddit)

	return payload.
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
		MutableContent().
		Sound("traloop.wav")
}
