package worker

import (
	"context"
	"fmt"
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

const (
	backoff      = 5 // How long we wait in between checking for notifications, in seconds
	pollDuration = 5 * time.Millisecond
	rate         = 0.1
)

type notificationsWorker struct {
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
}

func NewNotificationsWorker(logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
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

	return &notificationsWorker{
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
	}
}

func (nw *notificationsWorker) Start() error {
	queue, err := nw.queue.OpenQueue("notifications")
	if err != nil {
		return err
	}

	nw.logger.WithFields(logrus.Fields{
		"numConsumers": nw.consumers,
	}).Info("starting up notifications worker")

	prefetchLimit := int64(nw.consumers * 2)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < nw.consumers; i++ {
		name := fmt.Sprintf("consumer %s-%d", host, i)

		consumer := NewNotificationsConsumer(nw, i)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			return err
		}
	}

	return nil
}

func (nw *notificationsWorker) Stop() {
	<-nw.queue.StopAllConsuming() // wait for all Consume() calls to finish
}

type notificationsConsumer struct {
	*notificationsWorker
	tag int

	apnsSandbox    *apns2.Client
	apnsProduction *apns2.Client
}

func NewNotificationsConsumer(nw *notificationsWorker, tag int) *notificationsConsumer {
	return &notificationsConsumer{
		nw,
		tag,
		apns2.NewTokenClient(nw.apns),
		apns2.NewTokenClient(nw.apns).Production(),
	}
}

func (nc *notificationsConsumer) Consume(delivery rmq.Delivery) {
	ctx := context.Background()

	defer func() {
		lockKey := fmt.Sprintf("locks:accounts:%s", delivery.Payload())
		if err := nc.redis.Del(ctx, lockKey).Err(); err != nil {
			nc.logger.WithFields(logrus.Fields{
				"lockKey": lockKey,
				"err":     err,
			}).Error("failed to remove lock")
		}
	}()

	nc.logger.WithFields(logrus.Fields{
		"account#id": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		nc.logger.WithFields(logrus.Fields{
			"account#id": delivery.Payload(),
			"err":        err,
		}).Error("failed to parse account ID")

		delivery.Reject()
		return
	}

	defer delivery.Ack()

	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	account, err := nc.accountRepo.GetByID(ctx, id)
	if err != nil {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
			"err":              err,
		}).Error("failed to fetch account from database")
		return
	}

	previousLastCheckedAt := account.LastCheckedAt
	newAccount := (previousLastCheckedAt == 0)
	account.LastCheckedAt = now

	if err = nc.accountRepo.Update(ctx, &account); err != nil {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
			"err":              err,
		}).Error("failed to update last_checked_at for account")
		return
	}

	rac := nc.reddit.NewAuthenticatedClient(account.RefreshToken, account.AccessToken)
	if account.ExpiresAt < int64(now) {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
		}).Debug("refreshing reddit token")

		tokens, err := rac.RefreshTokens()
		if err != nil {
			if err != reddit.ErrOauthRevoked {
				nc.logger.WithFields(logrus.Fields{
					"account#username": account.NormalizedUsername(),
					"err":              err,
				}).Error("failed to refresh reddit tokens")
				return
			}

			err = nc.deleteAccount(ctx, account)
			if err != nil {
				nc.logger.WithFields(logrus.Fields{
					"account#username": account.NormalizedUsername(),
					"err":              err,
				}).Error("failed to remove revoked account")
				return
			}
		}

		// Update account
		account.AccessToken = tokens.AccessToken
		account.RefreshToken = tokens.RefreshToken
		account.ExpiresAt = int64(now + 3540)

		// Refresh client
		rac = nc.reddit.NewAuthenticatedClient(tokens.RefreshToken, tokens.AccessToken)

		if err = nc.accountRepo.Update(ctx, &account); err != nil {
			nc.logger.WithFields(logrus.Fields{
				"account#username": account.NormalizedUsername(),
				"err":              err,
			}).Error("failed to update reddit tokens for account")
			return
		}
	}

	// Only update delay on accounts we can actually check, otherwise it skews
	// the numbers too much.
	if !newAccount {
		latency := now - previousLastCheckedAt - float64(backoff)
		nc.statsd.Histogram("apollo.queue.delay", latency, []string{}, rate)
	}

	nc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
	}).Debug("fetching message inbox")

	opts := []reddit.RequestOption{reddit.WithQuery("limit", "10")}
	if account.LastMessageID != "" {
		opts = append(opts, reddit.WithQuery("before", account.LastMessageID))
	}
	msgs, err := rac.MessageInbox(opts...)

	if err != nil {
		switch err {
		case reddit.ErrTimeout: // Don't log timeouts
			break
		case reddit.ErrOauthRevoked:
			err = nc.deleteAccount(ctx, account)
			if err != nil {
				nc.logger.WithFields(logrus.Fields{
					"account#username": account.NormalizedUsername(),
					"err":              err,
				}).Error("failed to remove revoked account")
				return
			}
			nc.logger.WithFields(logrus.Fields{
				"account#username": account.NormalizedUsername(),
			}).Info("removed revoked account")
			break
		default:
			nc.logger.WithFields(logrus.Fields{
				"account#username": account.NormalizedUsername(),
				"err":              err,
			}).Error("failed to fetch message inbox")
		}
		return
	}

	// Figure out where we stand
	if msgs.Count == 0 {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
		}).Debug("no new messages, bailing early")
		return
	}

	nc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
		"count":            msgs.Count,
	}).Debug("fetched messages")

	account.LastMessageID = msgs.Children[0].FullName()

	if err = nc.accountRepo.Update(ctx, &account); err != nil {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
			"err":              err,
		}).Error("failed to update last_message_id for account")
		return
	}

	// Let's populate this with the latest message so we don't flood users with stuff
	if newAccount {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
		}).Debug("populating first message ID to prevent spamming")
		return
	}

	devices, err := nc.deviceRepo.GetByAccountID(ctx, account.ID)
	if err != nil {
		nc.logger.WithFields(logrus.Fields{
			"account#username": account.NormalizedUsername(),
			"err":              err,
		}).Error("failed to fetch account devices")
		return
	}

	// Iterate backwards so we notify from older to newer
	for i := msgs.Count - 1; i >= 0; i-- {
		msg := msgs.Children[i]
		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromMessage(account, msg, msgs.Count)

		for _, device := range devices {
			notification.DeviceToken = device.APNSToken
			client := nc.apnsProduction
			if device.Sandbox {
				client = nc.apnsSandbox
			}

			res, err := client.Push(notification)
			if err != nil {
				nc.statsd.Incr("apns.notification.errors", []string{}, 1)
				nc.logger.WithFields(logrus.Fields{
					"account#username": account.NormalizedUsername(),
					"err":              err,
					"status":           res.StatusCode,
					"reason":           res.Reason,
				}).Error("failed to send notification")
			} else {
				nc.statsd.Incr("apns.notification.sent", []string{}, 1)
				nc.logger.WithFields(logrus.Fields{
					"account#username": account.NormalizedUsername(),
					"token":            device.APNSToken,
				}).Info("sent notification")
			}
		}
	}

	ev := fmt.Sprintf("Sent notification to /u/%s (x%d)", account.Username, msgs.Count)
	nc.statsd.SimpleEvent(ev, "")

	nc.logger.WithFields(logrus.Fields{
		"account#username": account.NormalizedUsername(),
	}).Debug("finishing job")
}

func (nc *notificationsConsumer) deleteAccount(ctx context.Context, account domain.Account) error {
	// Disassociate account from devices
	devs, err := nc.deviceRepo.GetByAccountID(ctx, account.ID)
	if err != nil {
		return err
	}

	for _, dev := range devs {
		if err := nc.accountRepo.Disassociate(ctx, &account, &dev); err != nil {
			return err
		}
	}

	return nc.accountRepo.Delete(ctx, account.ID)
}

func payloadFromMessage(acct domain.Account, msg *reddit.Thing, badgeCount int) *payload.Payload {
	postBody := msg.Body
	if len(postBody) > 2000 {
		postBody = msg.Body[:2000]
	}

	postTitle := msg.LinkTitle
	if postTitle == "" {
		postTitle = msg.Subject
	}
	if len(postTitle) > 75 {
		postTitle = fmt.Sprintf("%s…", postTitle[0:75])
	}

	payload := payload.
		NewPayload().
		AlertBody(postBody).
		AlertSummaryArg(msg.Author).
		Badge(badgeCount).
		Custom("account_id", acct.AccountID).
		Custom("author", msg.Author).
		Custom("destination_author", msg.Destination).
		Custom("parent_id", msg.ParentID).
		Custom("post_title", msg.LinkTitle).
		Custom("subreddit", msg.Subreddit).
		MutableContent().
		Sound("traloop.wav")

	switch {
	case (msg.Kind == "t1" && msg.Type == "username_mention"):
		title := fmt.Sprintf(`Mention in “%s”`, postTitle)
		payload = payload.AlertTitle(title).Custom("type", "username")

		pType, _ := reddit.SplitID(msg.ParentID)
		if pType == "t1" {
			payload = payload.Category("inbox-username-mention-context")
		} else {
			payload = payload.Category("inbox-username-mention-no-context")
		}

		payload = payload.Custom("subject", "comment").ThreadID("comment")
		break
	case (msg.Kind == "t1" && msg.Type == "post_reply"):
		title := fmt.Sprintf(`%s to “%s”`, msg.Author, postTitle)
		payload = payload.
			AlertTitle(title).
			Category("inbox-post-reply").
			Custom("post_id", msg.ID).
			Custom("subject", "comment").
			Custom("type", "post").
			ThreadID("comment")
		break
	case (msg.Kind == "t1" && msg.Type == "comment_reply"):
		title := fmt.Sprintf(`%s in “%s”`, msg.Author, postTitle)
		postID := reddit.PostIDFromContext(msg.Context)
		payload = payload.
			AlertTitle(title).
			Category("inbox-comment-reply").
			Custom("comment_id", msg.ID).
			Custom("post_id", postID).
			Custom("subject", "comment").
			Custom("type", "comment").
			ThreadID("comment")
		break
	case (msg.Kind == "t4"):
		title := fmt.Sprintf(`Message from %s`, msg.Author)
		payload = payload.
			AlertTitle(title).
			AlertSubtitle(postTitle).
			Category("inbox-private-message").
			Custom("type", "private-message")
		break
	}

	return payload
}
