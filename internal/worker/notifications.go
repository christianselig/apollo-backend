package worker

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sideshow/apns2/token"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

const (
	rate = 0.1

	postReplyNotificationTitleFormat       = "%s to %s"
	commentReplyNotificationTitleFormat    = "%s in %s"
	privateMessageNotificationTitleFormat  = "Message from %s"
	usernameMentionNotificationTitleFormat = "Mention in \u201c%s\u201d"
)

type notificationsWorker struct {
	context.Context

	logger *zap.Logger
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

func NewNotificationsWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
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

	return &notificationsWorker{
		ctx,
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

	nw.logger.Info("starting up notifications worker", zap.Int("consumers", nw.consumers))

	prefetchLimit := int64(nw.consumers * 4)

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
	id := delivery.Payload()
	key := fmt.Sprintf("locks:accounts:%s", id)

	// Measure queue latency
	ttl := nc.redis.TTL(nc, key).Val().Milliseconds()
	_ = nc.statsd.Histogram("apollo.dequeue.time", float64(ttl), []string{"queue:notifications"}, 1.0)

	defer func() {
		if err := nc.redis.Del(nc, key).Err(); err != nil {
			nc.logger.Error("failed to remove account lock", zap.Error(err), zap.String("key", key))
		}
	}()

	nc.logger.Debug("starting job", zap.String("account#reddit_account_id", id))

	defer func() {
		if err := delivery.Ack(); err != nil {
			nc.logger.Error("failed to acknowledge message", zap.Error(err), zap.String("account#reddit_account_id", id))
		}
	}()

	now := time.Now()

	account, err := nc.accountRepo.GetByRedditID(nc, id)
	if err != nil {
		nc.logger.Error("failed to fetch account from database", zap.Error(err), zap.String("account#reddit_account_id", id))
		return
	}

	account.CheckCount++
	newAccount := account.CheckCount == 1

	defer func(acc *domain.Account) {
		if err = nc.accountRepo.Update(nc, acc); err != nil {
			nc.logger.Error("failed to update account",
				zap.Error(err),
				zap.String("account#reddit_account_id", id),
				zap.String("account#username", account.NormalizedUsername()),
			)
		}
	}(&account)

	rac := nc.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)
	if account.TokenExpiresAt.Before(now.Add(5 * time.Minute)) {
		nc.logger.Debug("refreshing reddit token",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)

		tokens, err := rac.RefreshTokens(nc)
		if err != nil {
			if err != reddit.ErrOauthRevoked {
				nc.logger.Error("failed to refresh reddit tokens",
					zap.Error(err),
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
				return
			}

			err = nc.deleteAccount(account)
			if err != nil {
				nc.logger.Error("failed to remove revoked account",
					zap.Error(err),
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
			}

			return
		}

		// Update account
		account.AccessToken = tokens.AccessToken
		account.RefreshToken = tokens.RefreshToken
		account.TokenExpiresAt = now.Add(tokens.Expiry)

		// Refresh client
		rac = nc.reddit.NewAuthenticatedClient(account.AccountID, tokens.RefreshToken, tokens.AccessToken)
	}

	nc.logger.Debug("fetching message inbox", zap.String("account#reddit_account_id", id), zap.String("account#username", account.NormalizedUsername()))

	opts := []reddit.RequestOption{reddit.WithQuery("limit", "10")}
	if account.LastMessageID != "" {
		opts = append(opts, reddit.WithQuery("before", account.LastMessageID))
	}
	msgs, err := rac.MessageInbox(nc, opts...)

	if err != nil {
		switch err {
		case reddit.ErrTimeout, reddit.ErrRateLimited: // Don't log timeouts or rate limits
			break
		case reddit.ErrOauthRevoked:
			if err = nc.deleteAccount(account); err != nil {
				nc.logger.Error("failed to remove revoked account",
					zap.Error(err),
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
			} else {
				nc.logger.Info("removed revoked account",
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
				)
			}
		default:
			nc.logger.Error("failed to fetch message inbox",
				zap.Error(err),
				zap.String("account#reddit_account_id", id),
				zap.String("account#username", account.NormalizedUsername()),
			)
		}
		return
	}

	// Figure out where we stand
	if msgs.Count == 0 {
		nc.logger.Debug("no new messages, bailing early",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return
	}

	nc.logger.Debug("fetched messages",
		zap.String("account#reddit_account_id", id),
		zap.String("account#username", account.NormalizedUsername()),
		zap.Int("count", msgs.Count),
	)

	for _, msg := range msgs.Children {
		if !msg.IsDeleted() {
			account.LastMessageID = msg.FullName()
			break
		}
	}

	// Let's populate this with the latest message so we don't flood users with stuff
	if newAccount {
		nc.logger.Debug("populating first message id to prevent spamming",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return
	}

	devices, err := nc.deviceRepo.GetInboxNotifiableByAccountID(nc, account.ID)
	if err != nil {
		nc.logger.Error("failed to fetch account devices",
			zap.Error(err),
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return
	}

	if len(devices) == 0 {
		nc.logger.Debug("no notifiable devices, bailing early",
			zap.String("account#reddit_account_id", id),
			zap.String("account#username", account.NormalizedUsername()),
		)
		return
	}

	// Iterate backwards so we notify from older to newer
	for i := msgs.Count - 1; i >= 0; i-- {
		msg := msgs.Children[i]

		if msg.IsDeleted() {
			continue
		}

		// Latency is the time difference between the appearence of the new message and the
		// time we notified at.
		latency := now.Sub(msg.CreatedAt)
		_ = nc.statsd.Histogram("apollo.queue.delay", float64(latency.Milliseconds()), []string{}, 1.0)

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
				_ = nc.statsd.Incr("apns.notification.errors", []string{}, 1)
				nc.logger.Error("failed to send notification",
					zap.Error(err),
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
					zap.String("device#token", device.APNSToken),
				)

				// Delete device as notifications might have been disabled here
				_ = nc.deviceRepo.Delete(nc, device.APNSToken)
			} else if !res.Sent() {
				_ = nc.statsd.Incr("apns.notification.errors", []string{}, 1)
				nc.logger.Error("notification not sent",
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
					zap.String("device#token", device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)

				// Delete device as notifications might have been disabled here
				_ = nc.deviceRepo.Delete(nc, device.APNSToken)
			} else {
				_ = nc.statsd.Incr("apns.notification.sent", []string{}, 1)
				nc.logger.Info("sent notification",
					zap.String("account#reddit_account_id", id),
					zap.String("account#username", account.NormalizedUsername()),
					zap.String("device#token", device.APNSToken),
				)
			}
		}
	}

	ev := fmt.Sprintf("Sent notification to /u/%s (x%d)", account.Username, msgs.Count)
	_ = nc.statsd.SimpleEvent(ev, "")

	nc.logger.Debug("finishing job",
		zap.String("account#reddit_account_id", id),
		zap.String("account#username", account.NormalizedUsername()),
	)
}

func (nc *notificationsConsumer) deleteAccount(account domain.Account) error {
	// Disassociate account from devices
	devs, err := nc.deviceRepo.GetByAccountID(nc, account.ID)
	if err != nil {
		return err
	}

	for _, dev := range devs {
		if err := nc.accountRepo.Disassociate(nc, &account, &dev); err != nil {
			return err
		}
	}

	return nc.accountRepo.Delete(nc, account.ID)
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
		title := fmt.Sprintf(usernameMentionNotificationTitleFormat, postTitle)
		postID := reddit.PostIDFromContext(msg.Context)
		payload = payload.
			AlertTitle(title).
			Custom("comment_id", msg.ID).
			Custom("post_id", postID).
			Custom("subreddit", msg.Subreddit).
			Custom("type", "username")

		pType, _ := reddit.SplitID(msg.ParentID)
		if pType == "t1" {
			payload = payload.Category("inbox-username-mention-context")
		} else {
			payload = payload.Category("inbox-username-mention-no-context")
		}

		payload = payload.Custom("subject", "comment").ThreadID("comment")
	case (msg.Kind == "t1" && msg.Type == "post_reply"):
		title := fmt.Sprintf(postReplyNotificationTitleFormat, msg.Author, postTitle)
		postID := reddit.PostIDFromContext(msg.Context)
		payload = payload.
			AlertTitle(title).
			Category("inbox-post-reply").
			Custom("comment_id", msg.ID).
			Custom("post_id", postID).
			Custom("subject", "comment").
			Custom("subreddit", msg.Subreddit).
			Custom("type", "post").
			ThreadID("comment")
	case (msg.Kind == "t1" && msg.Type == "comment_reply"):
		title := fmt.Sprintf(commentReplyNotificationTitleFormat, msg.Author, postTitle)
		postID := reddit.PostIDFromContext(msg.Context)
		payload = payload.
			AlertTitle(title).
			Category("inbox-comment-reply").
			Custom("comment_id", msg.ID).
			Custom("post_id", postID).
			Custom("subject", "comment").
			Custom("subreddit", msg.Subreddit).
			Custom("type", "comment").
			ThreadID("comment")
	case (msg.Kind == "t4"):
		title := fmt.Sprintf(privateMessageNotificationTitleFormat, msg.Author)
		payload = payload.
			AlertTitle(title).
			AlertSubtitle(postTitle).
			Category("inbox-private-message").
			Custom("comment_id", msg.ID).
			Custom("type", "private-message")
	}

	return payload
}
