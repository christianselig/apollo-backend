package worker

import (
	"context"
	"fmt"
	"math/rand"
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
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type subredditsWorker struct {
	logger *logrus.Logger
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

func NewSubredditsWorker(logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
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

	sw.logger.WithFields(logrus.Fields{
		"numConsumers": sw.consumers,
	}).Info("starting up subreddits worker")

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
	ctx := context.Background()

	sc.logger.WithFields(logrus.Fields{
		"subreddit#id": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		sc.logger.WithFields(logrus.Fields{
			"subreddit#id": delivery.Payload(),
			"err":          err,
		}).Error("failed to parse subreddit ID")

		_ = delivery.Reject()
		return
	}

	defer func() { _ = delivery.Ack() }()

	subreddit, err := sc.subredditRepo.GetByID(ctx, id)
	if err != nil {
		sc.logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch subreddit from database")
		return
	}

	watchers, err := sc.watcherRepo.GetBySubredditID(ctx, subreddit.ID)
	if err != nil {
		sc.logger.WithFields(logrus.Fields{
			"subreddit#id": subreddit.ID,
			"err":          err,
		}).Error("failed to fetch watchers from database")
		return
	}

	if len(watchers) == 0 {
		sc.logger.WithFields(logrus.Fields{
			"subreddit#id": subreddit.ID,
			"err":          err,
		}).Info("no watchers for subreddit, skipping")
		return
	}

	threshold := float64(time.Now().AddDate(0, 0, -1).UTC().Unix())
	posts := []*reddit.Thing{}
	before := ""
	finished := false

	for pages := 0; pages < 5; pages++ {
		i := rand.Intn(len(watchers))
		watcher := watchers[i]

		dev, err := sc.deviceRepo.GetByID(ctx, watcher.DeviceID)
		if err != nil {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id": subreddit.ID,
				"watcher#id":   watcher.ID,
				"err":          err,
			}).Error("failed to fetch device for watcher from database")
			continue
		}

		accs, err := sc.accountRepo.GetByAPNSToken(ctx, dev.APNSToken)
		if err != nil {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id": subreddit.ID,
				"watcher#id":   watcher.ID,
				"device#id":    dev.ID,
				"err":          err,
			}).Error("failed to fetch accounts for device from database")
			continue
		}

		i = rand.Intn(len(accs))
		acc := accs[i]

		rac := sc.reddit.NewAuthenticatedClient(acc.RefreshToken, acc.AccessToken)

		sps, err := rac.SubredditNew(
			subreddit.Name,
			reddit.WithQuery("before", before),
			reddit.WithQuery("limit", "100"),
		)

		if err != nil {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id": subreddit.ID,
				"watcher#id":   watcher.ID,
				"device#id":    dev.ID,
				"err":          err,
			}).Error("failed to fetch posts")
			continue
		}

		// If it's empty, we're done
		if sps.Count == 0 {
			break
		}

		// If we don't have 100 posts, we're going to be done
		if sps.Count < 100 {
			finished = true
		}

		for _, post := range sps.Children {
			if post.CreatedAt < threshold {
				finished = true
				break
			}

			posts = append(posts, post)
		}

		if finished {
			break
		}
	}

	for _, post := range posts {
		ids := []int64{}

		for _, watcher := range watchers {
			matched := (watcher.Upvotes == 0 || (watcher.Upvotes > 0 && post.Score > watcher.Upvotes)) &&
				(watcher.Keyword == "" || strings.Contains(post.SelfText, watcher.Keyword)) &&
				(watcher.Flair == "" || strings.Contains(post.Flair, watcher.Flair)) &&
				(watcher.Domain == "" || strings.Contains(post.URL, watcher.Domain))

			if !matched {
				continue
			}

			lockKey := fmt.Sprintf("watcher:%d:%s", watcher.ID, post.ID)
			notified, _ := sc.redis.Get(ctx, lockKey).Bool()

			if notified {
				continue
			}

			sc.redis.SetEX(ctx, lockKey, true, 24*time.Hour)
			ids = append(ids, watcher.DeviceID)
		}

		if len(ids) == 0 {
			continue
		}

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromPost(post)

		for _, id := range ids {
			device, _ := sc.deviceRepo.GetByID(ctx, id)
			notification.DeviceToken = device.APNSToken

			client := sc.apnsProduction
			if device.Sandbox {
				client = sc.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil {
				_ = sc.statsd.Incr("apns.notification.errors", []string{}, 1)
				sc.logger.WithFields(logrus.Fields{
					"subreddit#id": subreddit.ID,
					"device#id":    device.ID,
					"err":          err,
					"status":       res.StatusCode,
					"reason":       res.Reason,
				}).Error("failed to send notification")
			} else {
				_ = sc.statsd.Incr("apns.notification.sent", []string{}, 1)
				sc.logger.WithFields(logrus.Fields{
					"subreddit#id": subreddit.ID,
					"device#id":    device.ID,
					"device#token": device.APNSToken,
				}).Info("sent notification")
			}
		}
	}
}

func payloadFromPost(post *reddit.Thing) *payload.Payload {
	payload := payload.
		NewPayload().
		AlertTitle("DING DONG").
		AlertBody("I got you something").
		AlertSummaryArg(post.Subreddit).
		Category("post-watch").
		Custom("post_title", post.Title).
		Custom("post_id", post.ID).
		Custom("author", post.Author).
		Custom("post_age", post.CreatedAt)

	return payload
}
