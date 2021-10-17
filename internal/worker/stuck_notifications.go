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
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type stuckNotificationsWorker struct {
	logger *logrus.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
	redis  *redis.Client
	queue  rmq.Connection
	reddit *reddit.Client

	consumers int

	accountRepo domain.AccountRepository
}

func NewStuckNotificationsWorker(logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		consumers,
	)

	return &stuckNotificationsWorker{
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

	snw.logger.WithFields(logrus.Fields{
		"numConsumers": snw.consumers,
	}).Info("starting up stuck notifications worker")

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
	ctx := context.Background()

	snc.logger.WithFields(logrus.Fields{
		"account#id": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		snc.logger.WithFields(logrus.Fields{
			"account#id": delivery.Payload(),
			"err":        err,
		}).Error("failed to parse account ID")

		_ = delivery.Reject()
		return
	}

	defer func() { _ = delivery.Ack() }()

	account, err := snc.accountRepo.GetByID(ctx, id)
	if err != nil {
		snc.logger.WithFields(logrus.Fields{
			"err": err,
		}).Error("failed to fetch account from database")
		return
	}

	if account.LastMessageID == "" {
		snc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
		}).Debug("account has no messages, returning")
		return
	}

	rac := snc.reddit.NewAuthenticatedClient(account.RefreshToken, account.AccessToken)

	snc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
		"thing#id":         account.LastMessageID,
	}).Debug("fetching last thing")

	kind := account.LastMessageID[:2]

	var things *reddit.ListingResponse
	if kind == "t4" {
		snc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
			"thing#id":         account.LastMessageID,
		}).Debug("checking last thing via inbox")

		things, err = rac.MessageInbox()
		if err != nil {
			snc.logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("failed to fetch last thing via inbox")
			return
		}
	} else {
		things, err = rac.AboutInfo(account.LastMessageID)
		if err != nil {
			snc.logger.WithFields(logrus.Fields{
				"err": err,
			}).Error("failed to fetch last thing")
			return
		}
	}

	if things.Count > 0 {
		for _, thing := range things.Children {
			if thing.FullName() != account.LastMessageID {
				continue
			}

			if !thing.IsDeleted() {
				snc.logger.WithFields(logrus.Fields{
					"account#username": account.NormalizedUsername(),
					"thing#id":         account.LastMessageID,
				}).Debug("thing exists, returning")
				return
			}
		}
	}

	snc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
		"thing#id":         account.LastMessageID,
	}).Info("thing got deleted, resetting")

	if kind != "t4" {
		snc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
		}).Info("getting message inbox to determine last good thing")

		things, err = rac.MessageInbox()
		if err != nil {
			snc.logger.WithFields(logrus.Fields{
				"account#username": account.NormalizedUsername(),
				"err":              err,
			}).Error("failed to get message inbox")
			return
		}
	}

	account.LastMessageID = ""

	snc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
	}).Debug("calculating last good thing")
	for _, thing := range things.Children {
		if thing.IsDeleted() {
			snc.logger.WithFields(logrus.Fields{
				"account#username": account.NormalizedUsername(),
				"thing#id":         thing.FullName(),
			}).Debug("thing deleted, next")
			continue
		}

		account.LastMessageID = thing.FullName()
		break
	}

	snc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
		"thing#id":         account.LastMessageID,
	}).Debug("updating last good thing")

	if err := snc.accountRepo.Update(ctx, &account); err != nil {
		snc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
			"err":              err,
		}).Error("failed to update account's message id")
	}
}
