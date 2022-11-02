package worker

import (
	"context"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type stuckNotificationsWorker struct {
	logger      *zap.Logger
	statsd      *statsd.Client
	db          *pgxpool.Pool
	redis       *redis.Client
	reddit      *reddit.Client
	accountRepo domain.AccountRepository
}

func NewStuckNotificationsWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		redis,
		consumers,
	)

	return &stuckNotificationsWorker{
		logger,
		statsd,
		db,
		redis,
		reddit,
		repository.NewPostgresAccount(db),
	}
}

func (snw *stuckNotificationsWorker) Process(ctx context.Context, args ...interface{}) error {
	now := time.Now()
	defer func() {
		elapsed := time.Now().Sub(now).Milliseconds()
		_ = snw.statsd.Histogram("apollo.consumer.runtime", float64(elapsed), []string{"queue:stuck-notifications"}, 0.1)
	}()

	id := args[0].(string)
	snw.logger.Debug("starting job", zap.String("account#reddit_account_id", id))

	account, err := snw.accountRepo.GetByRedditID(ctx, id)
	if err != nil {
		snw.logger.Error("failed to fetch account from database", zap.Error(err), zap.String("account#reddit_account_id", id))
		return nil
	}

	if account.LastMessageID == "" {
		snw.logger.Debug("account has no messages, bailing early",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return nil
	}

	rac := snw.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)

	snw.logger.Debug("fetching last thing",
		zap.String("account#reddit_account_id", id),
		zap.String("account#username", account.NormalizedUsername()),
	)

	kind := account.LastMessageID[:2]

	var things *reddit.ListingResponse
	if kind == "t4" {
		snw.logger.Debug("checking last thing via inbox",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)

		things, err = rac.MessageInbox(ctx)
		if err != nil {
			if err != reddit.ErrRateLimited && err != reddit.ErrOauthRevoked {
				snw.logger.Error("failed to fetch last thing via inbox",
					zap.Error(err),
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
				return err
			}

			return nil
		}
	} else {
		things, err = rac.AboutInfo(ctx, account.LastMessageID)
		if err != nil {
			snw.logger.Error("failed to fetch last thing",
				zap.Error(err),
				zap.String("account#reddit_account_id", id),
				zap.String("account#username", account.NormalizedUsername()),
			)
			return nil
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
				return nil
			}

			sthings, err := rac.MessageInbox(ctx)
			if err != nil {
				snw.logger.Error("failed to check inbox",
					zap.Error(err),
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
				return nil
			}

			found := false
			for _, sthing := range sthings.Children {
				if sthing.FullName() == account.LastMessageID {
					found = true
				}
			}

			if !found {
				snw.logger.Debug("thing exists, but not on inbox, marking as deleted",
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
					zap.String("thing#id", account.LastMessageID),
				)
				break
			}

			snw.logger.Debug("thing exists, bailing early",
				zap.String("account#reddit_account_id", id),
				zap.String("account#username", account.NormalizedUsername()),
				zap.String("thing#id", account.LastMessageID),
			)
			return nil
		}
	}

	snw.logger.Info("thing got deleted, resetting",
		zap.String("account#reddit_account_id", id),
		zap.String("account#username", account.NormalizedUsername()),
		zap.String("thing#id", account.LastMessageID),
	)

	if kind != "t4" {
		snw.logger.Debug("getting message inbox to find last good thing",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)

		things, err = rac.MessageInbox(ctx)
		if err != nil {
			snw.logger.Error("failed to check inbox",
				zap.Error(err),
				zap.String("account#reddit_account_id", id),
				zap.String("account#username", account.NormalizedUsername()),
			)
			return nil
		}
	}

	account.LastMessageID = ""

	snw.logger.Debug("calculating last good thing",
		zap.String("account#reddit_account_id", id),
		zap.String("account#username", account.NormalizedUsername()),
	)
	for _, thing := range things.Children {
		if thing.IsDeleted() {
			snw.logger.Debug("thing got deleted, checking next",
				zap.String("account#reddit_account_id", id),
				zap.String("account#username", account.NormalizedUsername()),
				zap.String("thing#id", thing.FullName()),
			)
			continue
		}

		account.LastMessageID = thing.FullName()
		break
	}

	snw.logger.Debug("updating last good thing",
		zap.String("account#reddit_account_id", id),
		zap.String("account#username", account.NormalizedUsername()),
		zap.String("thing#id", account.LastMessageID),
	)

	if err := snw.accountRepo.Update(ctx, &account); err != nil {
		snw.logger.Error("failed to update account's last message id",
			zap.Error(err),
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return err
	}

	return nil
}
