package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v5"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type usersWorker struct {
	context.Context

	logger *zap.Logger
	tracer trace.Tracer
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

const userNotificationTitleFormat = "👨\u200d🚀 %s"

func NewUsersWorker(ctx context.Context, logger *zap.Logger, tracer trace.Tracer, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		tracer,
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

	return &usersWorker{
		ctx,
		logger,
		tracer,
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

	uw.logger.Info("starting up subreddits worker", zap.Int("consumers", uw.consumers))

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
	ctx, cancel := context.WithCancel(uc)
	defer cancel()

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		uc.logger.Error("failed to parse subreddit id from payload", zap.Error(err), zap.String("payload", delivery.Payload()))
		_ = delivery.Reject()
		return
	}

	uc.logger.Debug("starting job", zap.Int64("subreddit#id", id))

	defer func() { _ = delivery.Ack() }()

	user, err := uc.userRepo.GetByID(ctx, id)
	if err != nil {
		uc.logger.Error("failed to fetch user from database", zap.Error(err), zap.Int64("subreddit#id", id))
		return
	}

	watchers, err := uc.watcherRepo.GetByUserID(ctx, user.ID)
	if err != nil {
		uc.logger.Error("failed to fetch watchers from database",
			zap.Error(err),
			zap.Int64("user#id", id),
			zap.String("user#name", user.NormalizedName()),
		)
		return
	}

	if len(watchers) == 0 {
		uc.logger.Debug("no watchers for user, bailing early",
			zap.Int64("user#id", id),
			zap.String("user#name", user.NormalizedName()),
		)
		return
	}

	// Load 25 newest posts
	i := rand.Intn(len(watchers))
	watcher := watchers[i]

	acc, _ := uc.accountRepo.GetByID(ctx, watcher.AccountID)
	rac := uc.reddit.NewAuthenticatedClient(acc.AccountID, acc.RefreshToken, acc.AccessToken)

	ru, err := rac.UserAbout(ctx, user.Name)
	if err != nil {
		uc.logger.Error("failed to fetch user details",
			zap.Error(err),
			zap.Int64("user#id", id),
			zap.String("user#name", user.NormalizedName()),
		)
		return
	}

	if !ru.AcceptFollowers {
		uc.logger.Info("user disabled followers, removing",
			zap.Int64("user#id", id),
			zap.String("user#name", user.NormalizedName()),
		)

		if err := uc.watcherRepo.DeleteByTypeAndWatcheeID(ctx, domain.UserWatcher, user.ID); err != nil {
			uc.logger.Error("failed to remove watchers for user who disallows followers",
				zap.Error(err),
				zap.Int64("user#id", id),
				zap.String("user#name", user.NormalizedName()),
			)
			return
		}

		if err := uc.userRepo.Delete(ctx, user.ID); err != nil {
			uc.logger.Error("failed to remove user",
				zap.Error(err),
				zap.Int64("user#id", id),
				zap.String("user#name", user.NormalizedName()),
			)
			return
		}
	}

	posts, err := rac.UserPosts(ctx, user.Name)
	if err != nil {
		uc.logger.Error("failed to fetch user activity",
			zap.Error(err),
			zap.Int64("user#id", id),
			zap.String("user#name", user.NormalizedName()),
		)
		return
	}

	for _, post := range posts.Children {
		lowcaseSubreddit := strings.ToLower(post.Subreddit)

		if post.SubredditType == "private" {
			continue
		}

		notifs := []domain.Watcher{}

		for _, watcher := range watchers {
			// Make sure we only alert on activities created after the search
			if watcher.CreatedAt.After(post.CreatedAt) {
				continue
			}

			if watcher.LastNotifiedAt.After(post.CreatedAt) {
				continue
			}

			if watcher.Subreddit != "" && lowcaseSubreddit != watcher.Subreddit {
				continue
			}

			notifs = append(notifs, watcher)
		}

		if len(notifs) == 0 {
			continue
		}

		payload := payloadFromUserPost(post)

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"

		for _, watcher := range notifs {
			if err := uc.watcherRepo.IncrementHits(ctx, watcher.ID); err != nil {
				uc.logger.Error("failed to increment watcher hits",
					zap.Error(err),
					zap.Int64("user#id", id),
					zap.String("user#name", user.NormalizedName()),
					zap.Int64("watcher#id", watcher.ID),
				)
				return
			}

			device, _ := uc.deviceRepo.GetByID(ctx, watcher.DeviceID)

			title := fmt.Sprintf(userNotificationTitleFormat, watcher.Label)
			payload.AlertTitle(title)

			notification.Payload = payload
			notification.DeviceToken = device.APNSToken

			client := uc.apnsProduction
			if device.Sandbox {
				client = uc.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil || !res.Sent() {
				_ = uc.statsd.Incr("apns.notification.errors", []string{}, 1)
				uc.logger.Error("failed to send notification",
					zap.Error(err),
					zap.Int64("user#id", id),
					zap.String("user#name", user.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("apns", watcher.Device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)
			} else {
				_ = uc.statsd.Incr("apns.notification.sent", []string{}, 1)
				uc.logger.Info("sent notification",
					zap.Int64("user#id", id),
					zap.String("user#name", user.NormalizedName()),
					zap.String("post#id", post.ID),
					zap.String("device#token", watcher.Device.APNSToken),
				)
			}
		}
	}

	uc.logger.Debug("finishing job",
		zap.Int64("user#id", id),
		zap.String("user#name", user.NormalizedName()),
	)
}

func payloadFromUserPost(post *reddit.Thing) *payload.Payload {
	payload := payload.
		NewPayload().
		AlertBody(post.Title).
		AlertSubtitle(post.Author).
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
