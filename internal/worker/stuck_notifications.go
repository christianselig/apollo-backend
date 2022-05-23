package worker

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type stuckNotificationsWorker struct {
	context.Context

	logger *zap.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
	redis  *redis.Client
	queue  rmq.Connection
	reddit *reddit.Client

	consumers int

	accountRepo domain.AccountRepository
}

func NewStuckNotificationsWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		redis,
		consumers,
	)

	return &stuckNotificationsWorker{
		ctx,
		logger,
		statsd,
		db,
		redis,
		queue,
		reddit,
		consumers,

		repository.NewPostgresAccount(db),
	}
}

func (snw *stuckNotificationsWorker) Start() error {
	queue, err := snw.queue.OpenQueue("stuck-notifications")
	if err != nil {
		return err
	}

	snw.logger.Info("starting up stuck notifications worker", zap.Int("consumers", snw.consumers))

	prefetchLimit := int64(snw.consumers * 2)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < snw.consumers; i++ {
		name := fmt.Sprintf("consumer %s-%d", host, i)

		consumer := NewStuckNotificationsConsumer(snw, i)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			return err
		}
	}

	return nil
}

func (snw *stuckNotificationsWorker) Stop() {
	<-snw.queue.StopAllConsuming() // wait for all Consume() calls to finish
}

type stuckNotificationsConsumer struct {
	*stuckNotificationsWorker
	tag int
}

func NewStuckNotificationsConsumer(snw *stuckNotificationsWorker, tag int) *stuckNotificationsConsumer {
	return &stuckNotificationsConsumer{
		snw,
		tag,
	}
}

func (snc *stuckNotificationsConsumer) Consume(delivery rmq.Delivery) {
	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		snc.logger.Error("failed to parse account id from payload", zap.Error(err), zap.String("payload", delivery.Payload()))

		_ = delivery.Reject()
		return
	}

	snc.logger.Debug("starting job", zap.Int64("account#id", id))

	defer func() { _ = delivery.Ack() }()

	account, err := snc.accountRepo.GetByID(snc, id)
	if err != nil {
		snc.logger.Error("failed to fetch account from database", zap.Error(err), zap.Int64("account#id", id))
		return
	}

	if account.LastMessageID == "" {
		snc.logger.Debug("account has no messages, bailing early",
			zap.Int64("account#id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return
	}

	rac := snc.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)

	snc.logger.Debug("fetching last thing",
		zap.Int64("account#id", id),
		zap.String("account#username", account.NormalizedUsername()),
	)

	kind := account.LastMessageID[:2]

	var things *reddit.ListingResponse
	if kind == "t4" {
		snc.logger.Debug("checking last thing via inbox",
			zap.Int64("account#id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)

		things, err = rac.MessageInbox(snc)
		if err != nil {
			if err != reddit.ErrRateLimited {
				snc.logger.Error("failed to fetch last thing via inbox",
					zap.Error(err),
					zap.Int64("account#id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
			}
			return
		}
	} else {
		things, err = rac.AboutInfo(snc, account.LastMessageID)
		if err != nil {
			snc.logger.Error("failed to fetch last thing",
				zap.Error(err),
				zap.Int64("account#id", id),
				zap.String("account#username", account.NormalizedUsername()),
			)
			return
		}
	}

	if things.Count > 0 {
		for _, thing := range things.Children {
			if thing.FullName() != account.LastMessageID {
				continue
			}

			if thing.IsDeleted() {
				break
			}

			if kind == "t4" {
				return
			}

			sthings, err := rac.MessageInbox(snc)
			if err != nil {
				snc.logger.Error("failed to check inbox",
					zap.Error(err),
					zap.Int64("account#id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
				return
			}

			found := false
			for _, sthing := range sthings.Children {
				if sthing.FullName() == account.LastMessageID {
					found = true
				}
			}

			if !found {
				snc.logger.Debug("thing exists, but not on inbox, marking as deleted",
					zap.Int64("account#id", id),
					zap.String("account#username", account.NormalizedUsername()),
					zap.String("thing#id", account.LastMessageID),
				)
				break
			}

			snc.logger.Debug("thing exists, bailing early",
				zap.Int64("account#id", id),
				zap.String("account#username", account.NormalizedUsername()),
				zap.String("thing#id", account.LastMessageID),
			)
			return
		}
	}

	snc.logger.Info("thing got deleted, resetting",
		zap.Int64("account#id", id),
		zap.String("account#username", account.NormalizedUsername()),
		zap.String("thing#id", account.LastMessageID),
	)

	if kind != "t4" {
		snc.logger.Debug("getting message inbox to find last good thing",
			zap.Int64("account#id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)

		things, err = rac.MessageInbox(snc)
		if err != nil {
			snc.logger.Error("failed to check inbox",
				zap.Error(err),
				zap.Int64("account#id", id),
				zap.String("account#username", account.NormalizedUsername()),
			)
			return
		}
	}

	account.LastMessageID = ""

	snc.logger.Debug("calculating last good thing",
		zap.Int64("account#id", id),
		zap.String("account#username", account.NormalizedUsername()),
	)
	for _, thing := range things.Children {
		if thing.IsDeleted() {
			snc.logger.Debug("thing got deleted, checking next",
				zap.Int64("account#id", id),
				zap.String("account#username", account.NormalizedUsername()),
				zap.String("thing#id", thing.FullName()),
			)
			continue
		}

		account.LastMessageID = thing.FullName()
		break
	}

	snc.logger.Debug("updating last good thing",
		zap.Int64("account#id", id),
		zap.String("account#username", account.NormalizedUsername()),
		zap.String("thing#id", account.LastMessageID),
	)

	if err := snc.accountRepo.Update(snc, &account); err != nil {
		snc.logger.Error("failed to update account's last message id",
			zap.Error(err),
			zap.Int64("account#id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
	}
}
