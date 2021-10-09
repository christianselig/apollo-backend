package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"

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

type usersWorker struct {
	logger *logrus.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
	redis  *redis.Client
	queue  rmq.Connection
	reddit *reddit.Client
	apns   *token.Token

	consumers int

	accountRepo domain.AccountRepository
	deviceRepo  domain.DeviceRepository
	userRepo    domain.UserRepository
	watcherRepo domain.WatcherRepository
}

func NewUsersWorker(logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
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

	return &usersWorker{
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
		repository.NewPostgresUser(db),
		repository.NewPostgresWatcher(db),
	}
}

func (uw *usersWorker) Start() error {
	queue, err := uw.queue.OpenQueue("users")
	if err != nil {
		return err
	}

	uw.logger.WithFields(logrus.Fields{
		"numConsumers": uw.consumers,
	}).Info("starting up users worker")

	prefetchLimit := int64(uw.consumers * 2)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < uw.consumers; i++ {
		name := fmt.Sprintf("consumer %s-%d", host, i)

		consumer := NewUsersConsumer(uw, i)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			return err
		}
	}

	return nil
}

func (uw *usersWorker) Stop() {
	<-uw.queue.StopAllConsuming() // wait for all Consume() calls to finish
}

type usersConsumer struct {
	*usersWorker
	tag int

	apnsSandbox    *apns2.Client
	apnsProduction *apns2.Client
}

func NewUsersConsumer(uw *usersWorker, tag int) *usersConsumer {
	return &usersConsumer{
		uw,
		tag,
		apns2.NewTokenClient(uw.apns),
		apns2.NewTokenClient(uw.apns).Production(),
	}
}

func (uc *usersConsumer) Consume(delivery rmq.Delivery) {
	ctx := context.Background()

	uc.logger.WithFields(logrus.Fields{
		"user#id": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		uc.logger.WithFields(logrus.Fields{
			"user#id": delivery.Payload(),
			"err":     err,
		}).Error("failed to parse user ID")

		_ = delivery.Reject()
		return
	}

	defer func() { _ = delivery.Ack() }()

	user, err := uc.userRepo.GetByID(ctx, id)
	if err != nil {
		uc.logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch user from database")
		return
	}

	watchers, err := uc.watcherRepo.GetByUserID(ctx, user.ID)
	if err != nil {
		uc.logger.WithFields(logrus.Fields{
			"user#id": user.ID,
			"err":     err,
		}).Error("failed to fetch watchers from database")
		return
	}

	if len(watchers) == 0 {
		uc.logger.WithFields(logrus.Fields{
			"user#id": user.ID,
			"err":     err,
		}).Info("no watchers for user, skipping")
		return
	}

	// Load 25 newest posts
	i := rand.Intn(len(watchers))
	watcher := watchers[i]

	acc, _ := uc.accountRepo.GetByID(ctx, watcher.AccountID)
	rac := uc.reddit.NewAuthenticatedClient(acc.RefreshToken, acc.AccessToken)

	posts, err := rac.UserPosts(user.Name)
	if err != nil {
		uc.logger.WithFields(logrus.Fields{
			"user#id": user.ID,
			"err":     err,
		}).Error("failed to fetch user activity")
		return
	}

	for _, post := range posts.Children {
		if post.CreatedAt < user.LastCheckedAt {
			break
		}

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromUserPost(post)

		for _, watcher := range watchers {
			// Make sure we only alert on activities created after the search
			if watcher.CreatedAt > post.CreatedAt {
				continue
			}
			device, _ := uc.deviceRepo.GetByID(ctx, watcher.DeviceID)
			notification.DeviceToken = device.APNSToken

			client := uc.apnsProduction
			if device.Sandbox {
				client = uc.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil {
				_ = uc.statsd.Incr("apns.notification.errors", []string{}, 1)
				uc.logger.WithFields(logrus.Fields{
					"user#id":   user.ID,
					"device#id": device.ID,
					"err":       err,
					"status":    res.StatusCode,
					"reason":    res.Reason,
				}).Error("failed to send notification")
			} else {
				_ = uc.statsd.Incr("apns.notification.sent", []string{}, 1)
				uc.logger.WithFields(logrus.Fields{
					"user#id":      user.ID,
					"device#id":    device.ID,
					"device#token": device.APNSToken,
				}).Info("sent notification")
			}
		}
	}

	uc.logger.WithFields(logrus.Fields{
		"user#id":   user.ID,
		"user#name": user.Name,
	}).Debug("finishing job")
}

func payloadFromUserPost(post *reddit.Thing) *payload.Payload {
	title := fmt.Sprintf("👨\u200d🚀 User post! (u/%s)", post.Author)

	payload := payload.
		NewPayload().
		AlertTitle(title).
		AlertBody(post.Title).
		AlertSummaryArg(post.Author).
		Category("user-watch").
		Custom("post_title", post.Title).
		Custom("post_id", post.ID).
		Custom("subreddit", post.Subreddit).
		Custom("author", post.Author).
		Custom("post_age", post.CreatedAt).
		MutableContent().
		Sound("traloop.wav")

	return payload
}
