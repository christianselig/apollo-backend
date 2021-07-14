package reddit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fastjson"
)

func TestMeResponseParsing(t *testing.T) {
	bb := []byte(`
{
  "is_employee": false,
  "seen_layout_switch": true,
  "has_visited_new_profile": true,
  "pref_no_profanity": false,
  "has_external_account": false,
  "pref_geopopular": "GLOBAL",
  "seen_redesign_modal": true,
  "pref_show_trending": true,
  "subreddit": {
    "default_set": true,
    "user_is_contributor": false,
    "banner_img": "",
    "restrict_posting": true,
    "user_is_banned": false,
    "free_form_reports": true,
    "community_icon": null,
    "show_media": true,
    "icon_color": "#EA0027",
    "user_is_muted": false,
    "display_name": "u_changelog",
    "header_img": null,
    "title": "",
    "coins": 0,
    "previous_names": [],
    "over_18": false,
    "icon_size": [
      256,
      256
    ],
    "primary_color": "",
    "icon_img": "https://www.redditstatic.com/avatars/avatar_default_19_EA0027.png",
    "description": "",
    "submit_link_label": "",
    "header_size": null,
    "restrict_commenting": false,
    "subscribers": 3,
    "submit_text_label": "",
    "is_default_icon": true,
    "link_flair_position": "",
    "display_name_prefixed": "u/changelog",
    "key_color": "",
    "name": "t5_c30sw",
    "is_default_banner": true,
    "url": "/user/changelog/",
    "quarantine": false,
    "banner_size": null,
    "user_is_moderator": true,
    "public_description": "",
    "link_flair_enabled": false,
    "disable_contributor_requests": false,
    "subreddit_type": "user",
    "user_is_subscriber": false
  },
  "pref_show_presence": false,
  "snoovatar_img": "",
  "snoovatar_size": null,
  "gold_expiration": null,
  "has_gold_subscription": false,
  "is_sponsor": false,
  "num_friends": 7,
  "features": {
    "mod_service_mute_writes": true,
    "promoted_trend_blanks": true,
    "show_amp_link": true,
    "chat": true,
    "mweb_link_tab": {
      "owner": "growth",
      "variant": "control_1",
      "experiment_id": 404
    },
    "is_email_permission_required": true,
    "mod_awards": true,
    "mweb_xpromo_revamp_v3": {
      "owner": "growth",
      "variant": "treatment_4",
      "experiment_id": 480
    },
    "chat_subreddit": true,
    "awards_on_streams": true,
    "webhook_config": true,
    "mweb_xpromo_modal_listing_click_daily_dismissible_ios": true,
    "live_orangereds": true,
    "cookie_consent_banner": true,
    "modlog_copyright_removal": true,
    "do_not_track": true,
    "mod_service_mute_reads": true,
    "chat_user_settings": true,
    "use_pref_account_deployment": true,
    "mweb_xpromo_interstitial_comments_ios": true,
    "noreferrer_to_noopener": true,
    "premium_subscriptions_table": true,
    "mweb_xpromo_interstitial_comments_android": true,
    "mweb_nsfw_xpromo": {
      "owner": "growth",
      "variant": "control_2",
      "experiment_id": 361
    },
    "mweb_xpromo_modal_listing_click_daily_dismissible_android": true,
    "mweb_sharing_web_share_api": {
      "owner": "growth",
      "variant": "control_1",
      "experiment_id": 314
    },
    "chat_group_rollout": true,
    "resized_styles_images": true,
    "spez_modal": true,
    "mweb_sharing_clipboard": {
      "owner": "growth",
      "variant": "control_2",
      "experiment_id": 315
    },
    "expensive_coins_package": true
  },
  "can_edit_name": false,
  "verified": true,
  "new_modmail_exists": null,
  "pref_autoplay": false,
  "coins": 0,
  "has_paypal_subscription": false,
  "has_subscribed_to_premium": false,
  "id": "1ia22",
  "has_stripe_subscription": false,
  "oauth_client_id": "5JHxEu-4wnFfBA",
  "can_create_subreddit": true,
  "over_18": true,
  "is_gold": false,
  "is_mod": false,
  "awarder_karma": 0,
  "suspension_expiration_utc": null,
  "has_verified_email": true,
  "is_suspended": false,
  "pref_video_autoplay": false,
  "in_chat": true,
  "has_android_subscription": false,
  "in_redesign_beta": true,
  "icon_img": "https://www.redditstatic.com/avatars/avatar_default_19_EA0027.png",
  "has_mod_mail": false,
  "pref_nightmode": true,
  "awardee_karma": 10,
  "hide_from_robots": true,
  "password_set": true,
  "link_karma": 2058,
  "force_password_reset": false,
  "total_karma": 3445,
  "seen_give_award_tooltip": false,
  "inbox_count": 0,
  "seen_premium_adblock_modal": false,
  "pref_top_karma_subreddits": false,
  "has_mail": false,
  "pref_show_snoovatar": false,
  "name": "changelog",
  "pref_clickgadget": 5,
  "created": 1176750666.0,
  "gold_creddits": 0,
  "created_utc": 1176721866.0,
  "has_ios_subscription": false,
  "pref_show_twitter": false,
  "in_beta": true,
  "comment_karma": 1377,
  "has_subscribed": true,
  "linked_identities": [],
  "seen_subreddit_chat_ftux": false
}
	`)

	parser := &fastjson.Parser{}
	val, err := parser.ParseBytes(bb)
	assert.NoError(t, err)

	me := NewMeResponse(val)
	assert.NotNil(t, me)

	assert.Equal(t, "1ia22", me.ID)
	assert.Equal(t, "changelog", me.Name)
}
