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
	seenPosts := map[string]bool{}

	// Load 500 newest posts
	sc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
	}).Debug("loading up to 500 new posts")

	for page := 0; page < 5; page++ {
		sc.logger.WithFields(logrus.Fields{
			"subreddit#id":   subreddit.ID,
			"subreddit#name": subreddit.Name,
			"page":           page,
		}).Debug("loading new posts")

		i := rand.Intn(len(watchers))
		watcher := watchers[i]

		acc, _ := sc.accountRepo.GetByID(ctx, watcher.AccountID)
		rac := sc.reddit.NewAuthenticatedClient(acc.RefreshToken, acc.AccessToken)

		sps, err := rac.SubredditNew(
			subreddit.Name,
			reddit.WithQuery("before", before),
			reddit.WithQuery("limit", "100"),
		)

		if err != nil {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id": subreddit.ID,
				"err":          err,
			}).Error("failed to fetch new posts")
			continue
		}

		sc.logger.WithFields(logrus.Fields{
			"subreddit#id":   subreddit.ID,
			"subreddit#name": subreddit.Name,
			"count":          sps.Count,
			"page":           page,
		}).Debug("loaded new posts for page")

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

			if _, ok := seenPosts[post.ID]; !ok {
				posts = append(posts, post)
				seenPosts[post.ID] = true
			}
		}

		if finished {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id":   subreddit.ID,
				"subreddit#name": subreddit.Name,
				"page":           page,
			}).Debug("reached date threshold")
			break
		}
	}

	// Load hot posts
	sc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
	}).Debug("loading hot posts")
	{
		i := rand.Intn(len(watchers))
		watcher := watchers[i]

		acc, _ := sc.accountRepo.GetByID(ctx, watcher.AccountID)
		rac := sc.reddit.NewAuthenticatedClient(acc.RefreshToken, acc.AccessToken)
		sps, err := rac.SubredditHot(
			subreddit.Name,
			reddit.WithQuery("limit", "100"),
		)

		if err != nil {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id": subreddit.ID,
				"err":          err,
			}).Error("failed to fetch hot posts")
		} else {
			sc.logger.WithFields(logrus.Fields{
				"subreddit#id":   subreddit.ID,
				"subreddit#name": subreddit.Name,
				"count":          sps.Count,
			}).Debug("loaded hot posts")

			for _, post := range sps.Children {
				if post.CreatedAt < threshold {
					break
				}
				if _, ok := seenPosts[post.ID]; !ok {
					posts = append(posts, post)
					seenPosts[post.ID] = true
				}
			}
		}
	}

	sc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
		"count":          len(posts),
	}).Debug("checking posts for hits")
	for _, post := range posts {
		ids := []int64{}

		for _, watcher := range watchers {
			// Make sure we only alert on posts created after the search
			if watcher.CreatedAt > post.CreatedAt {
				continue
			}

			matched := true

			if watcher.Upvotes > 0 && post.Score < watcher.Upvotes {
				matched = false
			}

			if watcher.Keyword != "" && !strings.Contains(post.Title, watcher.Keyword) {
				matched = false
			}

			if watcher.Flair != "" && !strings.Contains(post.Flair, watcher.Flair) {
				matched = false
			}

			if watcher.Domain != "" && !strings.Contains(post.URL, watcher.Domain) {
				matched = false
			}

			if !matched {
				continue
			}

			lockKey := fmt.Sprintf("watcher:%d:%s", watcher.DeviceID, post.ID)
			notified, _ := sc.redis.Get(ctx, lockKey).Bool()

			if notified {
				sc.logger.WithFields(logrus.Fields{
					"subreddit#id":   subreddit.ID,
					"subreddit#name": subreddit.Name,
					"watcher#id":     watcher.ID,
					"post#id":        post.ID,
				}).Debug("already notified, skipping")

				continue
			}

			sc.logger.WithFields(logrus.Fields{
				"subreddit#id":   subreddit.ID,
				"subreddit#name": subreddit.Name,
				"watcher#id":     watcher.ID,
				"post#id":        post.ID,
			}).Debug("got a hit")

			sc.redis.SetEX(ctx, lockKey, true, 24*time.Hour)
			ids = append(ids, watcher.DeviceID)
		}

		if len(ids) == 0 {
			continue
		}

		sc.logger.WithFields(logrus.Fields{
			"subreddit#id":   subreddit.ID,
			"subreddit#name": subreddit.Name,
			"post#id":        post.ID,
			"count":          len(ids),
		}).Debug("got hits for post")

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

	sc.logger.WithFields(logrus.Fields{
		"subreddit#id":   subreddit.ID,
		"subreddit#name": subreddit.Name,
	}).Debug("finishing job")
}

func payloadFromPost(post *reddit.Thing) *payload.Payload {
	payload := payload.
		NewPayload().
		AlertTitle(post.Title).
		AlertSubtitle(fmt.Sprintf("in r/%s", post.Subreddit)).
		AlertSummaryArg(post.Subreddit).
		Category("post-watch").
		Custom("post_title", post.Title).
		Custom("post_id", post.ID).
		Custom("subreddit", post.Subreddit).
		Custom("author", post.Author).
		Custom("post_age", post.CreatedAt).
		MutableContent().
		Sound("traloop.wav")

	return payload
}
