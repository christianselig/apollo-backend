package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/DataDog/datadog-go/statsd"
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
	CommentID        string `json:"commentId,omitempty"`
	CommentAuthor    string `json:"commentAuthor,omitempty"`
	CommentBody      string `json:"commentBody,omitempty"`
	CommentAge       int64  `json:"commentAge,omitempty"`
	CommentScore     int64  `json:"commentScore,omitempty"`
}

type liveActivitiesWorker struct {
	logger           *zap.Logger
	statsd           *statsd.Client
	db               *pgxpool.Pool
	redis            *redis.Client
	reddit           *reddit.Client
	apns             *apns2.Client
	liveActivityRepo domain.LiveActivityRepository
}

func NewLiveActivitiesWorker(ctx context.Context, logger *zap.Logger, statsd *statsd.Client, db *pgxpool.Pool, redis *redis.Client, consumers int) Worker {
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

	return &liveActivitiesWorker{
		logger,
		statsd,
		db,
		redis,
		reddit,
		apns,
		repository.NewPostgresLiveActivity(db),
	}
}

func (law *liveActivitiesWorker) Process(ctx context.Context, args ...interface{}) error {
	now := time.Now()
	defer func() {
		elapsed := time.Now().Sub(now).Milliseconds()
		_ = law.statsd.Histogram("apollo.consumer.runtime", float64(elapsed), []string{"queue:live_activities"}, 0.1)
	}()

	at := args[0].(string)
	key := fmt.Sprintf("locks:live-activities:%s", at)

	// Measure queue latency
	ttl := law.redis.PTTL(ctx, key).Val()
	age := (domain.NotificationCheckTimeout - ttl)
	_ = law.statsd.Histogram("apollo.dequeue.latency", float64(age.Milliseconds()), []string{"queue:live_activities"}, 0.1)

	defer func() {
		if err := law.redis.Del(ctx, key).Err(); err != nil {
			law.logger.Error("failed to remove account lock", zap.Error(err), zap.String("key", key))
		}
	}()

	law.logger.Debug("starting job", zap.String("live_activity#apns_token", at))

	la, err := law.liveActivityRepo.Get(ctx, at)
	if err != nil {
		law.logger.Error("failed to get live activity", zap.Error(err), zap.String("live_activity#apns_token", at))
		return err
	}

	rac := law.reddit.NewAuthenticatedClient(la.RedditAccountID, la.RefreshToken, la.AccessToken)
	if la.TokenExpiresAt.Before(now.Add(5 * time.Minute)) {
		law.logger.Debug("refreshing reddit token",
			zap.String("live_activity#apns_token", at),
			zap.String("reddit#id", la.RedditAccountID),
			zap.String("reddit#access_token", rac.ObfuscatedAccessToken()),
			zap.String("reddit#refresh_token", rac.ObfuscatedRefreshToken()),
		)

		tokens, err := rac.RefreshTokens(ctx)
		if err != nil {
			law.logger.Error("failed to refresh reddit tokens",
				zap.Error(err),
				zap.String("live_activity#apns_token", at),
				zap.String("reddit#id", la.RedditAccountID),
				zap.String("reddit#access_token", rac.ObfuscatedAccessToken()),
				zap.String("reddit#refresh_token", rac.ObfuscatedRefreshToken()),
			)
			if err == reddit.ErrOauthRevoked {
				_ = law.liveActivityRepo.Delete(ctx, at)
				return nil
			}

			return err
		}

		// Update account
		la.AccessToken = tokens.AccessToken
		la.RefreshToken = tokens.RefreshToken
		la.TokenExpiresAt = now.Add(tokens.Expiry)
		_ = law.liveActivityRepo.Update(ctx, &la)

		// Refresh client
		rac = law.reddit.NewAuthenticatedClient(la.RedditAccountID, tokens.RefreshToken, tokens.AccessToken)
	}

	law.logger.Debug("fetching latest comments", zap.String("live_activity#apns_token", at))

	tr, err := rac.TopLevelComments(ctx, la.Subreddit, la.ThreadID)
	if err != nil {
		law.logger.Error("failed to fetch latest comments",
			zap.Error(err),
			zap.String("live_activity#apns_token", at),
			zap.String("reddit#id", la.RedditAccountID),
			zap.String("reddit#access_token", rac.ObfuscatedAccessToken()),
			zap.String("reddit#refresh_token", rac.ObfuscatedRefreshToken()),
		)
		if err == reddit.ErrOauthRevoked {
			_ = law.liveActivityRepo.Delete(ctx, at)
			return nil
		}

		return err
	}

	if len(tr.Children) == 0 && la.ExpiresAt.After(now) {
		law.logger.Debug("no comments found", zap.String("live_activity#apns_token", at))
		return nil
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
		law.logger.Debug("no new comments found", zap.String("live_activity#apns_token", at))
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	din := DynamicIslandNotification{
		PostCommentCount: tr.Post.NumComments,
		PostScore:        tr.Post.Score,
	}

	if len(candidates) > 0 {
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

	bb, _ := json.Marshal(map[string]interface{}{
		"aps": map[string]interface{}{
			"content-state":  din,
			"dismissal-date": la.ExpiresAt.Unix(),
			"event":          ev,
			"timestamp":      now.Unix(),
		},
	})

	notification := &apns2.Notification{
		DeviceToken: la.APNSToken,
		Topic:       "com.christianselig.Apollo.push-type.liveactivity",
		PushType:    "liveactivity",
		Payload:     bb,
	}

	res, err := law.apns.PushWithContext(ctx, notification)
	if err != nil {
		_ = law.statsd.Incr("apns.live_activities.errors", []string{}, 1)
		law.logger.Error("failed to send notification",
			zap.Error(err),
			zap.String("live_activity#apns_token", at),
			zap.Bool("live_activity#sandbox", la.Sandbox),
			zap.String("notification#type", ev),
		)

		_ = law.liveActivityRepo.Delete(ctx, at)
	} else if !res.Sent() {
		_ = law.statsd.Incr("apns.live_activities.errors", []string{}, 1)
		law.logger.Error("notification not sent",
			zap.String("live_activity#apns_token", at),
			zap.Bool("live_activity#sandbox", la.Sandbox),
			zap.String("notification#type", ev),
			zap.Int("response#status", res.StatusCode),
			zap.String("response#reason", res.Reason),
		)

		_ = law.liveActivityRepo.Delete(ctx, at)
	} else {
		_ = law.statsd.Incr("apns.notification.sent", []string{}, 1)
		law.logger.Debug("sent notification",
			zap.String("live_activity#apns_token", at),
			zap.Bool("live_activity#sandbox", la.Sandbox),
			zap.String("notification#type", ev),
		)
	}

	if la.ExpiresAt.Before(now) {
		law.logger.Debug("live activity expired, deleting", zap.String("live_activity#apns_token", at))
		_ = law.liveActivityRepo.Delete(ctx, at)
	}

	law.logger.Debug("finishing job",
		zap.String("live_activity#apns_token", at),
	)
	return nil
}
