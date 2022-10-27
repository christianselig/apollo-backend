package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/token"
	"go.uber.org/zap"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
	"github.com/christianselig/apollo-backend/internal/repository"
)

type DynamicIslandNotification struct {
	PostCommentCount int    `json:"postTotalComments"`
	PostScore        int64  `json:"postScore"`
	CommentID        string `json:"commentId"`
	CommentAuthor    string `json:"commentAuthor"`
	CommentBody      string `json:"commentBody"`
	CommentAge       int64  `json:"commentAge"`
	CommentScore     int64  `json:"commentScore"`
}

type liveActivitiesWorker struct {
	context.Context

	logger *zap.Logger
	statsd *statsd.Client
	db     *pgxpool.Pool
	redis  *redis.Client
	queue  rmq.Connection
	reddit *reddit.Client
	apns   *token.Token

	consumers int

	liveActivityRepo domain.LiveActivityRepository
}

func NewLiveActivitiesWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, queue rmq.Connection, consumers int) Worker {
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

	return &liveActivitiesWorker{
		ctx,
		logger,
		statsd,
		db,
		redis,
		queue,
		reddit,
		apns,
		consumers,

		repository.NewPostgresLiveActivity(db),
	}
}

func (law *liveActivitiesWorker) Start() error {
	queue, err := law.queue.OpenQueue("live-activities")
	if err != nil {
		return err
	}

	law.logger.Info("starting up live activities worker", zap.Int("consumers", law.consumers))

	prefetchLimit := int64(law.consumers * 4)

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		return err
	}

	host, _ := os.Hostname()

	for i := 0; i < law.consumers; i++ {
		name := fmt.Sprintf("consumer %s-%d", host, i)

		consumer := NewLiveActivitiesConsumer(law, i)
		if _, err := queue.AddConsumer(name, consumer); err != nil {
			return err
		}
	}

	return nil
}

func (law *liveActivitiesWorker) Stop() {
	<-law.queue.StopAllConsuming() // wait for all Consume() calls to finish
}

type liveActivitiesConsumer struct {
	*liveActivitiesWorker
	tag int

	apnsSandbox    *apns2.Client
	apnsProduction *apns2.Client
}

func NewLiveActivitiesConsumer(law *liveActivitiesWorker, tag int) *liveActivitiesConsumer {
	return &liveActivitiesConsumer{
		law,
		tag,
		apns2.NewTokenClient(law.apns),
		apns2.NewTokenClient(law.apns).Production(),
	}
}

func (lac *liveActivitiesConsumer) Consume(delivery rmq.Delivery) {
	ctx, cancel := context.WithCancel(lac)
	defer cancel()

	now := time.Now().UTC()
	defer func() {
		elapsed := time.Now().Sub(now).Milliseconds()
		_ = lac.statsd.Histogram("apollo.consumer.runtime", float64(elapsed), []string{"queue:live_activities"}, 0.1)
	}()

	at := delivery.Payload()
	key := fmt.Sprintf("locks:live-activities:%s", at)

	// Measure queue latency
	ttl := lac.redis.PTTL(ctx, key).Val()
	age := (domain.NotificationCheckTimeout - ttl)
	_ = lac.statsd.Histogram("apollo.dequeue.latency", float64(age.Milliseconds()), []string{"queue:live_activities"}, 0.1)

	defer func() {
		if err := lac.redis.Del(ctx, key).Err(); err != nil {
			lac.logger.Error("failed to remove account lock", zap.Error(err), zap.String("key", key))
		}
	}()

	lac.logger.Debug("starting job", zap.String("live_activity#apns_token", at))

	defer func() {
		if err := delivery.Ack(); err != nil {
			lac.logger.Error("failed to acknowledge message", zap.Error(err), zap.String("live_activity#apns_token", at))
		}
	}()

	la, err := lac.liveActivityRepo.Get(ctx, at)
	if err != nil {
		lac.logger.Error("failed to get live activity", zap.Error(err), zap.String("live_activity#apns_token", at))
		return
	}

	rac := lac.reddit.NewAuthenticatedClient(la.RedditAccountID, la.RefreshToken, la.AccessToken)
	if la.TokenExpiresAt.Before(now.Add(5 * time.Minute)) {
		lac.logger.Debug("refreshing reddit token",
			zap.String("live_activity#apns_token", at),
			zap.String("reddit#id", la.RedditAccountID),
			zap.String("reddit#access_token", rac.ObfuscatedAccessToken()),
			zap.String("reddit#refresh_token", rac.ObfuscatedRefreshToken()),
		)

		tokens, err := rac.RefreshTokens(ctx)
		if err != nil {
			lac.logger.Error("failed to refresh reddit tokens",
				zap.Error(err),
				zap.String("live_activity#apns_token", at),
				zap.String("reddit#id", la.RedditAccountID),
				zap.String("reddit#access_token", rac.ObfuscatedAccessToken()),
				zap.String("reddit#refresh_token", rac.ObfuscatedRefreshToken()),
			)
			if err == reddit.ErrOauthRevoked {
				_ = lac.liveActivityRepo.Delete(ctx, at)
			}
			return
		}

		// Update account
		la.AccessToken = tokens.AccessToken
		la.RefreshToken = tokens.RefreshToken
		la.TokenExpiresAt = now.Add(tokens.Expiry)
		_ = lac.liveActivityRepo.Update(ctx, &la)

		// Refresh client
		rac = lac.reddit.NewAuthenticatedClient(la.RedditAccountID, tokens.RefreshToken, tokens.AccessToken)
	}

	lac.logger.Debug("fetching latest comments", zap.String("live_activity#apns_token", at))

	tr, err := rac.TopLevelComments(ctx, la.Subreddit, la.ThreadID)
	if err != nil {
		lac.logger.Error("failed to fetch latest comments",
			zap.Error(err),
			zap.String("live_activity#apns_token", at),
			zap.String("reddit#id", la.RedditAccountID),
			zap.String("reddit#access_token", rac.ObfuscatedAccessToken()),
			zap.String("reddit#refresh_token", rac.ObfuscatedRefreshToken()),
		)
		if err == reddit.ErrOauthRevoked {
			_ = lac.liveActivityRepo.Delete(ctx, at)
		}
		return
	}

	if len(tr.Children) == 0 {
		lac.logger.Debug("no comments found", zap.String("live_activity#apns_token", at))
		return
	}

	// Filter out comments in the last minute
	candidates := make([]*reddit.Thing, 0)
	cutoffs := []time.Time{
		now.Add(-domain.LiveActivityCheckInterval),
		now.Add(-domain.LiveActivityCheckInterval * 2),
		now.Add(-domain.LiveActivityCheckInterval * 4),
	}

	for _, cutoff := range cutoffs {
		for _, t := range tr.Children {
			if t.CreatedAt.After(cutoff) {
				candidates = append(candidates, t)
			}
		}

		if len(candidates) > 0 {
			break
		}
	}

	if len(candidates) == 0 && la.ExpiresAt.After(now) {
		lac.logger.Debug("no new comments found", zap.String("live_activity#apns_token", at))
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	din := DynamicIslandNotification{
		PostCommentCount: tr.Post.NumComments,
		PostScore:        tr.Post.Score,
	}

	if len(candidates) > 1 {
		comment := candidates[0]

		din.CommentID = comment.ID
		din.CommentAuthor = comment.Author
		din.CommentBody = comment.Body
		din.CommentAge = comment.CreatedAt.Unix()
		din.CommentScore = comment.Score
	}

	ev := "update"
	if la.ExpiresAt.Before(now) {
		ev = "end"
	}

	pl := map[string]interface{}{
		"aps": map[string]interface{}{
			"timestamp":     time.Now().Unix(),
			"event":         ev,
			"content-state": din,
		},
	}
	bb, _ := json.Marshal(pl)

	notification := &apns2.Notification{
		DeviceToken: la.APNSToken,
		Topic:       "com.christianselig.Apollo.push-type.liveactivity",
		PushType:    "liveactivity",
		Payload:     bb,
	}

	client := lac.apnsProduction
	if la.Sandbox {
		client = lac.apnsSandbox
	}

	res, err := client.PushWithContext(ctx, notification)
	if err != nil {
		_ = lac.statsd.Incr("apns.live_activities.errors", []string{}, 1)
		lac.logger.Error("failed to send notification",
			zap.Error(err),
			zap.String("live_activity#apns_token", at),
			zap.String("notification#type", ev),
		)

		_ = lac.liveActivityRepo.Delete(ctx, at)
	} else if !res.Sent() {
		_ = lac.statsd.Incr("apns.live_activities.errors", []string{}, 1)
		lac.logger.Error("notification not sent",
			zap.String("live_activity#apns_token", at),
			zap.String("notification#type", ev),
			zap.Int("response#status", res.StatusCode),
			zap.String("response#reason", res.Reason),
		)

		_ = lac.liveActivityRepo.Delete(ctx, at)
	} else {
		_ = lac.statsd.Incr("apns.notification.sent", []string{}, 1)
		lac.logger.Info("sent notification",
			zap.String("live_activity#apns_token", at),
			zap.String("notification#type", ev),
		)
	}

	if la.ExpiresAt.Before(now) {
		lac.logger.Debug("live activity expired, deleting", zap.String("live_activity#apns_token", at))
		_ = lac.liveActivityRepo.Delete(ctx, at)
	}

	lac.logger.Debug("finishing job",
		zap.String("live_activity#apns_token", at),
	)
}
