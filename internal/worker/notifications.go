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
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"github.com/sirupsen/logrus"

	"github.com/christianselig/apollo-backend/internal/data"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

const (
	backoff      = 5 // How long we wait in between checking for notifications, in seconds
	pollDuration = 10 * time.Millisecond
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
}

func NewNotificationsWorker(logger *logrus.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
	)

	var apns *token.Token
	{
		authKey, err := token.AuthKeyFromBytes([]byte(os.Getenv("APPLE_KEY_PKEY")))
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
	}
}

func (nw *notificationsWorker) Start(consumers int) error {
	queue, err := nw.queue.OpenQueue("notifications")
	if err != nil {
		return err
	}

	nw.logger.WithFields(logrus.Fields{
		"numConsumers": consumers,
	}).Info("starting up notifications worker")

	prefetchLimit := int64(consumers * 32)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < consumers; i++ {
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
		"accountID": delivery.Payload(),
	}).Debug("starting job")

	id, err := strconv.ParseInt(delivery.Payload(), 10, 64)
	if err != nil {
		nc.logger.WithFields(logrus.Fields{
			"accountID": delivery.Payload(),
			"err":       err,
		}).Error("failed to parse account ID")

		delivery.Reject()
		return
	}

	defer delivery.Ack()

	now := float64(time.Now().UnixNano()/int64(time.Millisecond)) / 1000

	stmt := `SELECT
			id,
			username,
			account_id,
			access_token,
			refresh_token,
			expires_at,
			last_message_id,
			last_checked_at
		FROM accounts
		WHERE id = $1`
	account := &data.Account{}
	if err := nc.db.QueryRow(ctx, stmt, id).Scan(
		&account.ID,
		&account.Username,
		&account.AccountID,
		&account.AccessToken,
		&account.RefreshToken,
		&account.ExpiresAt,
		&account.LastMessageID,
		&account.LastCheckedAt,
	); err != nil {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to fetch account from database")
		return
	}

	if err = nc.db.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE accounts
			SET last_checked_at = $1
			WHERE id = $2`
		_, err := tx.Exec(ctx, stmt, now, account.ID)
		return err
	}); err != nil {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to update last_checked_at for account")
		return
	}

	rac := nc.reddit.NewAuthenticatedClient(account.RefreshToken, account.AccessToken)
	if account.ExpiresAt < int64(now) {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
		}).Debug("refreshing reddit token")

		tokens, err := rac.RefreshTokens()
		if err != nil {
			nc.logger.WithFields(logrus.Fields{
				"accountID": id,
				"err":       err,
			}).Error("failed to refresh reddit tokens")
			return
		}

		// Update account
		account.AccessToken = tokens.AccessToken
		account.RefreshToken = tokens.RefreshToken
		account.ExpiresAt = int64(now + 3540)

		// Refresh client
		rac = nc.reddit.NewAuthenticatedClient(tokens.RefreshToken, tokens.AccessToken)

		err = nc.db.BeginFunc(ctx, func(tx pgx.Tx) error {
			stmt := `
				UPDATE accounts
				SET access_token = $1, refresh_token = $2, expires_at = $3 WHERE id = $4`
			_, err := tx.Exec(ctx, stmt, account.AccessToken, account.RefreshToken, account.ExpiresAt, account.ID)
			return err
		})
		if err != nil {
			nc.logger.WithFields(logrus.Fields{
				"accountID": id,
				"err":       err,
			}).Error("failed to update reddit tokens for account")
			return
		}
	}

	// Only update delay on accounts we can actually check, otherwise it skews
	// the numbers too much.
	if account.LastCheckedAt > 0 {
		latency := now - account.LastCheckedAt - float64(backoff)
		nc.statsd.Histogram("apollo.queue.delay", latency, []string{}, rate)
	}

	nc.logger.WithFields(logrus.Fields{
		"accountID": id,
	}).Debug("fetching message inbox")
	msgs, err := rac.MessageUnread(account.LastMessageID)

	if err != nil {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to fetch message inbox")
		return
	}

	nc.logger.WithFields(logrus.Fields{
		"accountID": id,
		"count":     len(msgs.MessageListing.Messages),
	}).Debug("fetched messages")

	if len(msgs.MessageListing.Messages) == 0 {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
		}).Debug("no new messages, bailing early")
		return
	}

	// Set latest message we alerted on
	latestMsg := msgs.MessageListing.Messages[0]
	if err = nc.db.BeginFunc(ctx, func(tx pgx.Tx) error {
		stmt := `
			UPDATE accounts
			SET last_message_id = $1
			WHERE id = $2`
		_, err := tx.Exec(ctx, stmt, latestMsg.FullName(), account.ID)
		return err
	}); err != nil {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to update last_message_id for account")
		return
	}

	// Let's populate this with the latest message so we don't flood users with stuff
	if account.LastMessageID == "" {
		nc.logger.WithFields(logrus.Fields{
			"accountID": delivery.Payload(),
		}).Debug("populating first message ID to prevent spamming")
		return
	}

	devices := []data.Device{}
	stmt = `
		SELECT apns_token, sandbox
		FROM devices
		INNER JOIN devices_accounts ON devices.id = devices_accounts.device_id
		WHERE devices_accounts.account_id = $1`
	rows, err := nc.db.Query(ctx, stmt, account.ID)
	if err != nil {
		nc.logger.WithFields(logrus.Fields{
			"accountID": id,
			"err":       err,
		}).Error("failed to fetch account devices")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var device data.Device
		rows.Scan(&device.APNSToken, &device.Sandbox)
		devices = append(devices, device)
	}

	for _, msg := range msgs.MessageListing.Messages {
		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromMessage(account, &msg, len(msgs.MessageListing.Messages))

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
					"accountID": id,
					"err":       err,
					"status":    res.StatusCode,
					"reason":    res.Reason,
				}).Error("failed to send notification")
			} else {
				nc.statsd.Incr("apns.notification.sent", []string{}, 1)
				nc.logger.WithFields(logrus.Fields{
					"accountID":  delivery.Payload(),
					"token":      device.APNSToken,
					"redditUser": account.Username,
				}).Info("sent notification")
			}
		}
	}

	nc.logger.WithFields(logrus.Fields{
		"accountID": delivery.Payload(),
	}).Debug("finishing job")
}

func payloadFromMessage(acct *data.Account, msg *reddit.MessageData, badgeCount int) *payload.Payload {
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
