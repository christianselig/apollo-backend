package worker

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
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

var notificationTags = []string{"queue:notifications"}

type notificationsWorker struct {
	logger      *zap.Logger
	statsd      *statsd.Client
	db          *pgxpool.Pool
	redis       *redis.Client
	reddit      *reddit.Client
	apns        *apns2.Client
	accountRepo domain.AccountRepository
	deviceRepo  domain.DeviceRepository
}

func NewNotificationsWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, consumers int) Worker {
	reddit := reddit.NewClient(
		os.Getenv("REDDIT_CLIENT_ID"),
		os.Getenv("REDDIT_CLIENT_SECRET"),
		statsd,
		redis,
		consumers,
	)

	var apns *apns2.Client
	{
		authKey, err := token.AuthKeyFromFile(os.Getenv("APPLE_KEY_PATH"))
		if err != nil {
			panic(err)
		}

		tok := &token.Token{
			AuthKey: authKey,
			KeyID:   os.Getenv("APPLE_KEY_ID"),
			TeamID:  os.Getenv("APPLE_TEAM_ID"),
		}
		apns = apns2.NewTokenClient(tok).Production()
	}

	return &notificationsWorker{
		logger,
		statsd,
		db,
		redis,
		reddit,
		apns,
		repository.NewPostgresAccount(db),
		repository.NewPostgresDevice(db),
	}
}

func (nw *notificationsWorker) Process(ctx context.Context, args ...interface{}) error {
	now := time.Now()
	defer func() {
		elapsed := time.Now().Sub(now).Milliseconds()
		_ = nw.statsd.Histogram("apollo.consumer.runtime", float64(elapsed), notificationTags, 0.1)
		_ = nw.statsd.Incr("apollo.consumer.executions", notificationTags, 0.1)
	}()

	id := args[0].(string)
	logger := nw.logger.With(zap.String("account#reddit_account_id", id))

	// Measure queue latency
	key := fmt.Sprintf("locks:accounts:%s", id)
	ttl := nw.redis.PTTL(ctx, key).Val()
	age := (domain.NotificationCheckTimeout - ttl)
	_ = nw.statsd.Histogram("apollo.dequeue.latency", float64(age.Milliseconds()), notificationTags, 0.1)

	defer func() {
		if err := nw.redis.Del(ctx, key).Err(); err != nil {
			logger.Error("failed to remove account lock", zap.Error(err), zap.String("key", key))
		}
	}()

	logger.Debug("starting job")

	account, err := nw.accountRepo.GetByRedditID(ctx, id)
	if err != nil {
		logger.Info("account not found, exiting", zap.Error(err))
		return nil
	}

	rac := nw.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)
	logger = logger.With(
		zap.String("account#username", account.NormalizedUsername()),
		zap.String("account#access_token", rac.ObfuscatedAccessToken()),
		zap.String("account#refresh_token", rac.ObfuscatedRefreshToken()),
	)
	if account.TokenExpiresAt.Before(now.Add(5 * time.Minute)) {
		logger.Debug("refreshing reddit token")

		tokens, err := rac.RefreshTokens(ctx)
		if err != nil {
			if err != reddit.ErrOauthRevoked {
				logger.Error("failed to refresh reddit tokens", zap.Error(err))
				return err
			}

			if err = nw.deleteAccount(ctx, account); err != nil {
				logger.Error("failed to remove revoked account", zap.Error(err))
				return err
			}

			return nil
		}

		// Update account
		account.AccessToken = tokens.AccessToken
		account.RefreshToken = tokens.RefreshToken
		account.TokenExpiresAt = now.Add(tokens.Expiry)
		_ = nw.accountRepo.Update(ctx, &account)

		// Refresh client
		rac = nw.reddit.NewAuthenticatedClient(account.AccountID, tokens.RefreshToken, tokens.AccessToken)
		logger = logger.With(
			zap.String("account#access_token", rac.ObfuscatedAccessToken()),
			zap.String("account#refresh_token", rac.ObfuscatedRefreshToken()),
		)
	}

	logger.Debug("fetching message inbox")

	opts := []reddit.RequestOption{reddit.WithQuery("limit", "10")}
	if account.LastMessageID != "" {
		opts = append(opts, reddit.WithQuery("before", account.LastMessageID))
	}
	msgs, err := rac.MessageInbox(ctx, opts...)

	if err != nil {
		switch err {
		case reddit.ErrOauthRevoked:
			_ = nw.deleteAccount(ctx, account)
			logger.Info("removed revoked account")
			return nil
		default:
			logger.Error("failed to fetch message inbox", zap.Error(err))
			return err
		}
	}

	// Figure out where we stand
	if msgs.Count == 0 {
		logger.Debug("no new messages, bailing early")
		return nil
	}

	logger.Debug("fetched messages", zap.Int("count", msgs.Count))

	for _, msg := range msgs.Children {
		if !msg.IsDeleted() {
			account.LastMessageID = msg.FullName()
			_ = nw.accountRepo.Update(ctx, &account)
			break
		}
	}

	// Let's populate this with the latest message so we don't flood users with stuff
	if account.CheckCount == 0 {
		logger.Debug("populating first message id to prevent spamming")

		account.CheckCount = 1
		_ = nw.accountRepo.Update(ctx, &account)
		return nil
	}

	devices, err := nw.deviceRepo.GetInboxNotifiableByAccountID(ctx, account.ID)
	if err != nil {
		logger.Error("failed to fetch account devices", zap.Error(err))
		return nil
	}

	if len(devices) == 0 {
		logger.Debug("no notifiable devices, bailing early")
		return nil
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
		_ = nw.statsd.Histogram("apollo.queue.delay", float64(latency.Milliseconds()), []string{}, 1.0)

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.Payload = payloadFromMessage(account, msg, msgs.Count)

		for _, device := range devices {
			notification.DeviceToken = device.APNSToken

			res, err := nw.apns.PushWithContext(ctx, notification)
			if err != nil {
				_ = nw.statsd.Incr("apns.notification.errors", []string{}, 1)
				logger.Error("failed to send notification",
					zap.Error(err),
					zap.String("device#token", device.APNSToken),
				)

				// Delete device as notifications might have been disabled here
				_ = nw.deviceRepo.Delete(ctx, device.APNSToken)
			} else if !res.Sent() {
				_ = nw.statsd.Incr("apns.notification.errors", []string{}, 1)
				logger.Error("notification not sent",
					zap.String("device#token", device.APNSToken),
					zap.Int("response#status", res.StatusCode),
					zap.String("response#reason", res.Reason),
				)

				// Delete device as notifications might have been disabled here
				_ = nw.deviceRepo.Delete(ctx, device.APNSToken)
			} else {
				_ = nw.statsd.Incr("apns.notification.sent", []string{}, 1)
				logger.Info("sent notification", zap.String("device#token", device.APNSToken))
			}
		}
	}

	ev := fmt.Sprintf("Sent notification to /u/%s (x%d)", account.Username, msgs.Count)
	_ = nw.statsd.SimpleEvent(ev, "")

	logger.Debug("finishing job")
	return nil
}

func (nw *notificationsWorker) deleteAccount(ctx context.Context, account domain.Account) error {
	// Disassociate account from devices
	devs, err := nw.deviceRepo.GetByAccountID(ctx, account.ID)
	if err != nil {
		return err
	}

	for _, dev := range devs {
		if err := nw.accountRepo.Disassociate(ctx, &account, &dev); err != nil {
			return err
		}
	}

	return nw.accountRepo.Delete(ctx, account.ID)
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
