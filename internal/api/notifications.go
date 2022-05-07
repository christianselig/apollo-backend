package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/payload"
	"github.com/sirupsen/logrus"
)

const (
	commentReplyNotificationTitleFormat    = "%s in %s"
	postReplyNotificationTitleFormat       = "%s to %s"
	privateMessageNotificationTitleFormat  = "Message from %s"
	subredditNotificationBodyFormat        = "r/%s: \u201c%s\u201d"
	subredditNotificationTitleFormat       = "📣 \u201c%s\u201d Watcher"
	trendingNotificationTitleFormat        = "🔥 r/%s Trending"
	usernameMentionNotificationTitleFormat = "Mention in \u201c%s\u201d"
)

type notificationGenerator func(*payload.Payload)

func generateNotificationTester(a *api, fun notificationGenerator) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		ctx := context.Background()
		tok := vars["apns"]

		d, err := a.deviceRepo.GetByAPNSToken(ctx, tok)
		if err != nil {
			a.logger.WithFields(logrus.Fields{
				"err": err,
			}).Info("failed fetching device from database")
			a.errorResponse(w, r, 500, err.Error())
			return
		}

		p := payload.NewPayload()

		p.MutableContent().
			Sound("traloop.wav")

		fun(p)

		notification := &apns2.Notification{}
		notification.Topic = "com.christianselig.Apollo"
		notification.DeviceToken = d.APNSToken
		notification.Payload = p

		client := apns2.NewTokenClient(a.apns)
		if !d.Sandbox {
			client = client.Production()
		}

		if _, err := client.Push(notification); err != nil {
			a.logger.WithFields(logrus.Fields{
				"err": err,
			}).Info("failed to send test notification")
			a.errorResponse(w, r, 500, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func privateMessage(p *payload.Payload) {
	title := fmt.Sprintf(privateMessageNotificationTitleFormat, "welcomebot")

	p.AlertTitle(title).
		AlertBody("**Welcome to r/GriefSupport!**\n\nWe're glad you found us, but sad you needed to.  We're here to support you during whatever difficulties you're going through.").
		AlertSubtitle("Welcome to r/GriefSupport!").
		AlertSummaryArg("welcomebot").
		Category("inbox-private-message").
		Custom("account_id", "1ia22").
		Custom("author", "welcomebot").
		Custom("comment_id", "1d2oouy").
		Custom("destination_author", "changelog").
		Custom("parent_id", "").
		Custom("post_title", "").
		Custom("subreddit", "").
		Custom("type", "private-message")
}

func commentReply(p *payload.Payload) {
	title := fmt.Sprintf(commentReplyNotificationTitleFormat, "Equinox_Shift", "Protests set to disrupt Ottawa's downtown for 3rd straight weekend")

	p.AlertTitle(title).
		AlertBody("They don't even go here.").
		Category("inbox-comment-reply").
		Custom("account_id", "1ia22").
		Custom("author", "Equinox_Shift").
		Custom("comment_id", "hwp66zg").
		Custom("destination_author", "changelog").
		Custom("parent_id", "t1_hwonb97").
		Custom("post_id", "sqqk29").
		Custom("post_title", "Protests set to disrupt Ottawa's downtown for 3rd straight weekend").
		Custom("subject", "comment").
		Custom("subreddit", "ottawa").
		Custom("subreddit", "ottawa").
		Custom("type", "comment").
		ThreadID("comment")
}

func postReply(p *payload.Payload) {
	title := fmt.Sprintf(postReplyNotificationTitleFormat, "Ryfter", "Quest 2 use during chemo")

	p.AlertTitle(title).
		AlertBody("As others have said, [Real Fishing VR](https://www.oculus.com/experiences/quest/2582932495064035).  Especially if he likes to fish.  My dad and mom were blown away by it.").
		Category("inbox-comment-reply").
		Custom("account_id", "1ia22").
		Custom("author", "Ryfter").
		Custom("comment_id", "hyg01ip").
		Custom("destination_author", "changelog").
		Custom("parent_id", "t3_t0qn4z").
		Custom("post_id", "t0qn4z").
		Custom("post_title", "Quest 2 use during chemo").
		Custom("subject", "comment").
		Custom("subreddit", "OculusQuest2").
		Custom("subreddit", "OculusQuest2").
		Custom("type", "post").
		ThreadID("comment")
}

func usernameMention(p *payload.Payload) {
	title := fmt.Sprintf(usernameMentionNotificationTitleFormat, "testimg")

	p.AlertTitle(title).
		AlertBody("yo u/changelog what's good").
		Category("inbox-username-mention-no-context").
		Custom("account_id", "1ia22").
		Custom("author", "iamthatis").
		Custom("comment_id", "i6xobpa").
		Custom("destination_author", "changelog").
		Custom("parent_id", "t3_u02338").
		Custom("post_id", "u02338").
		Custom("post_title", "testimg").
		Custom("subject", "comment").
		Custom("subreddit", "calicosummer").
		Custom("subreddit", "calicosummer").
		Custom("type", "username")

}
func subredditWatcher(p *payload.Payload) {
	title := fmt.Sprintf(subredditNotificationTitleFormat, "bug pics")
	body := fmt.Sprintf(subredditNotificationBodyFormat, "pics", "A Goliath Stick Insect. Aware of my presence she let me get close enough for a photo. (OC)")

	p.AlertTitle(title).
		AlertBody(body).
		AlertSummaryArg("pics").
		Category("subreddit-watcher").
		Custom("author", "befarked247").
		Custom("post_age", 1651409659.0).
		Custom("post_id", "ufzaml").
		Custom("post_title", "A Goliath Stick Insect. Aware of my presence she let me get close enough for a photo. (OC)").
		Custom("subreddit", "pics").
		Custom("thumbnail", "https://a.thumbs.redditmedia.com/Lr4b-YHLTNu1LFuyUY1Zic8kHy3ojX06gLcZOuqxrr0.jpg").
		ThreadID("subreddit-watcher")
}

func trendingPost(p *payload.Payload) {
	title := fmt.Sprintf(trendingNotificationTitleFormat, "pics")

	p.AlertTitle(title).
		AlertBody("A Goliath Stick Insect. Aware of my presence she let me get close enough for a photo. (OC)").
		AlertSummaryArg("pics").
		Category("trending-post").
		Custom("author", "befarked247").
		Custom("post_age", 1651409659.0).
		Custom("post_id", "ufzaml").
		Custom("post_title", "A Goliath Stick Insect. Aware of my presence she let me get close enough for a photo. (OC)").
		Custom("subreddit", "pics").
		Custom("thumbnail", "https://a.thumbs.redditmedia.com/Lr4b-YHLTNu1LFuyUY1Zic8kHy3ojX06gLcZOuqxrr0.jpg").
		ThreadID("trending-post")
}
