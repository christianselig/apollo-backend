package apns2

import (
	"encoding/json"
	"time"
)

// EPushType defines the value for the apns-push-type header
type EPushType string

const (
	// PushTypeAlert is used for notifications that trigger a user interaction —
	// for example, an alert, badge, or sound. If you set this push type, the
	// apns-topic header field must use your app’s bundle ID as the topic. The
	// alert push type is required on watchOS 6 and later. It is recommended on
	// macOS, iOS, tvOS, and iPadOS.
	PushTypeAlert EPushType = "alert"

	// PushTypeBackground is used for notifications that deliver content in the
	// background, and don’t trigger any user interactions. If you set this push
	// type, the apns-topic header field must use your app’s bundle ID as the
	// topic. The background push type is required on watchOS 6 and later. It is
	// recommended on macOS, iOS, tvOS, and iPadOS.
	PushTypeBackground EPushType = "background"

	// PushTypeVOIP is used for notifications that provide information about an
	// incoming Voice-over-IP (VoIP) call. If you set this push type, the
	// apns-topic header field must use your app’s bundle ID with .voip appended
	// to the end. If you’re using certificate-based authentication, you must
	// also register the certificate for VoIP services. The voip push type is
	// not available on watchOS. It is recommended on macOS, iOS, tvOS, and
	// iPadOS.
	PushTypeVOIP EPushType = "voip"

	// PushTypeComplication is used for notifications that contain update
	// information for a watchOS app’s complications. If you set this push type,
	// the apns-topic header field must use your app’s bundle ID with
	// .complication appended to the end. If you’re using certificate-based
	// authentication, you must also register the certificate for WatchKit
	// services. The complication push type is recommended for watchOS and iOS.
	// It is not available on macOS, tvOS, and iPadOS.
	PushTypeComplication EPushType = "complication"

	// PushTypeFileProvider is used to signal changes to a File Provider
	// extension. If you set this push type, the apns-topic header field must
	// use your app’s bundle ID with .pushkit.fileprovider appended to the end.
	// The fileprovider push type is not available on watchOS. It is recommended
	// on macOS, iOS, tvOS, and iPadOS.
	PushTypeFileProvider EPushType = "fileprovider"

	// PushTypeMDM is used for notifications that tell managed devices to
	// contact the MDM server. If you set this push type, you must use the topic
	// from the UID attribute in the subject of your MDM push certificate.
	PushTypeMDM EPushType = "mdm"
)

const (
	// PriorityLow will tell APNs to send the push message at a time that takes
	// into account power considerations for the device. Notifications with this
	// priority might be grouped and delivered in bursts. They are throttled,
	// and in some cases are not delivered.
	PriorityLow = 5

	// PriorityHigh will tell APNs to send the push message immediately.
	// Notifications with this priority must trigger an alert, sound, or badge
	// on the target device. It is an error to use this priority for a push
	// notification that contains only the content-available key.
	PriorityHigh = 10
)

// Notification represents the the data and metadata for a APNs Remote Notification.
type Notification struct {

	// An optional canonical UUID that identifies the notification. The
	// canonical form is 32 lowercase hexadecimal digits, displayed in five
	// groups separated by hyphens in the form 8-4-4-4-12. An example UUID is as
	// follows:
	//
	//  123e4567-e89b-12d3-a456-42665544000
	//
	// If you don't set this, a new UUID is created by APNs and returned in the
	// response.
	ApnsID string

	// A string which allows multiple notifications with the same collapse
	// identifier to be displayed to the user as a single notification. The
	// value should not exceed 64 bytes.
	CollapseID string

	// A string containing hexadecimal bytes of the device token for the target
	// device.
	DeviceToken string

	// The topic of the remote notification, which is typically the bundle ID
	// for your app. The certificate you create in the Apple Developer Member
	// Center must include the capability for this topic. If your certificate
	// includes multiple topics, you must specify a value for this header. If
	// you omit this header and your APNs certificate does not specify multiple
	// topics, the APNs server uses the certificate’s Subject as the default
	// topic.
	Topic string

	// An optional time at which the notification is no longer valid and can be
	// discarded by APNs. If this value is in the past, APNs treats the
	// notification as if it expires immediately and does not store the
	// notification or attempt to redeliver it. If this value is left as the
	// default (ie, Expiration.IsZero()) an expiration header will not added to
	// the http request.
	Expiration time.Time

	// The priority of the notification. Specify ether apns.PriorityHigh (10) or
	// apns.PriorityLow (5) If you don't set this, the APNs server will set the
	// priority to 10.
	Priority int

	// A byte array containing the JSON-encoded payload of this push notification.
	// Refer to "The Remote Notification Payload" section in the Apple Local and
	// Remote Notification Programming Guide for more info.
	Payload interface{}

	// The pushtype of the push notification. If this values is left as the
	// default an apns-push-type header with value 'alert' will be added to the
	// http request.
	PushType EPushType
}

// MarshalJSON converts the notification payload to JSON.
func (n *Notification) MarshalJSON() ([]byte, error) {
	switch n.Payload.(type) {
	case string:
		return []byte(n.Payload.(string)), nil
	case []byte:
		return n.Payload.([]byte), nil
	default:
		return json.Marshal(n.Payload)
	}
}
