package itunes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type numericString string

func (n *numericString) UnmarshalJSON(b []byte) error {
	var number json.Number
	if err := json.Unmarshal(b, &number); err != nil {
		return err
	}
	*n = numericString(number.String())
	return nil
}

type Environment string

const (
	Sandbox    Environment = "Sandbox"
	Production Environment = "Production"
)

const (
	SubscriptionMonthly    = "MONTHLY"
	SubscriptionTriMonthly = "TRIMONTHLY"
	SubscriptionYearly     = "YEARLY"
	SubscriptionLifetime   = "LIFETIME"
)

// Models are courtesy of awa/go-iap https://github.com/awa/go-iap/blob/master/appstore/model.go
type (
	// The ReceiptCreationDate type indicates the date when the app receipt was created.
	ReceiptCreationDate struct {
		CreationDate    string `json:"receipt_creation_date"`
		CreationDateMS  string `json:"receipt_creation_date_ms"`
		CreationDatePST string `json:"receipt_creation_date_pst"`
	}

	// The RequestDate type indicates the date and time that the request was sent
	RequestDate struct {
		RequestDate    string `json:"request_date"`
		RequestDateMS  string `json:"request_date_ms"`
		RequestDatePST string `json:"request_date_pst"`
	}

	// The PurchaseDate type indicates the date and time that the item was purchased
	PurchaseDate struct {
		PurchaseDate    string `json:"purchase_date"`
		PurchaseDateMS  string `json:"purchase_date_ms"`
		PurchaseDatePST string `json:"purchase_date_pst"`
	}

	// The OriginalPurchaseDate type indicates the beginning of the subscription period
	OriginalPurchaseDate struct {
		OriginalPurchaseDate    string `json:"original_purchase_date"`
		OriginalPurchaseDateMS  string `json:"original_purchase_date_ms"`
		OriginalPurchaseDatePST string `json:"original_purchase_date_pst"`
	}

	// The ExpiresDate type indicates the expiration date for the subscription
	ExpiresDate struct {
		ExpiresDate             string `json:"expires_date,omitempty"`
		ExpiresDateMS           int64  `json:"expires_date_ms,string,omitempty"`
		ExpiresDatePST          string `json:"expires_date_pst,omitempty"`
		ExpiresDateFormatted    string `json:"expires_date_formatted,omitempty"`
		ExpiresDateFormattedPST string `json:"expires_date_formatted_pst,omitempty"`
	}

	// The CancellationDate type indicates the time and date of the cancellation by Apple customer support
	CancellationDate struct {
		CancellationDate    string `json:"cancellation_date,omitempty"`
		CancellationDateMS  string `json:"cancellation_date_ms,omitempty"`
		CancellationDatePST string `json:"cancellation_date_pst,omitempty"`
	}

	// The InApp type has the receipt attributes
	InApp struct {
		Quantity              string `json:"quantity"`
		ProductID             string `json:"product_id"`
		TransactionID         string `json:"transaction_id"`
		OriginalTransactionID string `json:"original_transaction_id"`
		WebOrderLineItemID    string `json:"web_order_line_item_id,omitempty"`

		IsTrialPeriod string `json:"is_trial_period"`
		ExpiresDate

		PurchaseDate
		OriginalPurchaseDate

		CancellationDate
		CancellationReason string `json:"cancellation_reason,omitempty"`
	}

	// The Receipt type has whole data of receipt
	Receipt struct {
		ReceiptType                string        `json:"receipt_type"`
		AdamID                     int64         `json:"adam_id"`
		AppItemID                  numericString `json:"app_item_id"`
		BundleID                   string        `json:"bundle_id"`
		ApplicationVersion         string        `json:"application_version"`
		DownloadID                 int64         `json:"download_id"`
		VersionExternalIdentifier  numericString `json:"version_external_identifier"`
		OriginalApplicationVersion string        `json:"original_application_version"`
		InApp                      []InApp       `json:"in_app"`
		ReceiptCreationDate
		RequestDate
		OriginalPurchaseDate
	}

	// A pending renewal may refer to a renewal that is scheduled in the future or a renewal that failed in the past for some reason.
	PendingRenewalInfo struct {
		SubscriptionExpirationIntent   string `json:"expiration_intent"`
		SubscriptionAutoRenewProductID string `json:"auto_renew_product_id"`
		SubscriptionRetryFlag          string `json:"is_in_billing_retry_period"`
		SubscriptionAutoRenewStatus    string `json:"auto_renew_status"`
		SubscriptionPriceConsentStatus string `json:"price_consent_status"`
		ProductID                      string `json:"product_id"`
	}

	// The IAPResponse type has the response properties
	// We defined each field by the current IAP response, but some fields are not mentioned
	// in the following Apple's document;
	// https://developer.apple.com/library/ios/releasenotes/General/ValidateAppStoreReceipt/Chapters/ReceiptFields.html
	// If you get other types or fields from the IAP response, you should use the struct you defined.
	IAPResponse struct {
		Status             int                  `json:"status"`
		Environment        Environment          `json:"environment"`
		Receipt            Receipt              `json:"receipt"`
		LatestReceiptInfo  []InApp              `json:"latest_receipt_info,omitempty"`
		LatestReceipt      string               `json:"latest_receipt,omitempty"`
		PendingRenewalInfo []PendingRenewalInfo `json:"pending_renewal_info,omitempty"`
		IsRetryable        bool                 `json:"is-retryable,omitempty"`
		VerificationInfo   ClientVerificationInfo
		DeleteDevice       bool
	}

	VerificationInfo struct {
		APNsToken string    `json:"apns_token"`
		Receipt   string    `json:"receipt"`
		Sandbox   bool      `json:"sandbox"`
		Accounts  []Account `json:"accounts"`
	}

	Account struct {
		AccountID    string `json:"account_id"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}

	ClientVerificationInfo struct {
		Products []VerificationProduct `json:"products"`
		Issue    string                `json:"issue,omitempty"`
	}

	VerificationProduct struct {
		Name             string `json:"name"`
		Status           string `json:"status"`
		SubscriptionType string `json:"subscription_type,omitempty"`
	}
)

func NewIAPResponse(receipt string, production bool) (*IAPResponse, error) {
	// Send the receipt data string off to Apple's servers to verify
	appleVerificationURL := "https://buy.itunes.apple.com/verifyReceipt"

	if !production {
		appleVerificationURL = "https://sandbox.itunes.apple.com/verifyReceipt"
	}

	verificationPayload := map[string]string{
		"receipt-data": receipt,
		"password":     "***REMOVED***",
	}

	bb, err := json.Marshal(verificationPayload)

	if err != nil {
		return nil, err
	}

	request, requestErr := http.NewRequest("POST", appleVerificationURL, bytes.NewBuffer(bb))

	if requestErr != nil {
		fmt.Println(requestErr)
	}

	request.Header.Set("Content-Type", "application/json; charset=utf-8")

	client := &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			IdleConnTimeout:       10 * time.Second,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
		},
	}

	resp, err := client.Do(request)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Printf("Weird HTTP status code from Apple: %d\n", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)
	iapr := &IAPResponse{}
	err = decoder.Decode(iapr)

	if err != nil {
		return nil, err
	}

	if iapr.Status == 21007 {
		// This is a sandbox receipt, reattempt with sandbox verification URL
		return NewIAPResponse(receipt, false)
	}

	iapr.handleAppleResponse()
	return iapr, nil
}

func (iapr *IAPResponse) handleAppleResponse() {
	// In the case the receipt is invalid or something similar, we don't want to send down empty products, as the client always expects entries for each product, then will look at the "issue" key if the receipt itself is flawed
	emptyUltraProduct := VerificationProduct{Name: "ultra", Status: "NO"}
	emptyProProduct := VerificationProduct{Name: "pro", Status: "NO"}
	emptyCommunityIconsProduct := VerificationProduct{Name: "community_icons", Status: "NO"}
	emptySPCAProduct := VerificationProduct{Name: "spca", Status: "NO"}
	emptyProducts := []VerificationProduct{emptyUltraProduct, emptyProProduct, emptyCommunityIconsProduct, emptySPCAProduct}

	if iapr.Status != 0 {
		if iapr.Status == 21000 || iapr.Status == 21002 || iapr.Status == 21003 || iapr.Status == 21004 || iapr.Status == 21005 || iapr.Status == 21009 {
			iapr.VerificationInfo = ClientVerificationInfo{Products: emptyProducts, Issue: "APPLE_ERROR"}
		} else if iapr.Status == 21008 || iapr.Status == 21009 {
			// Oops, we sent it to the wrong endpoint (sandbox when we should have done production, or vice-versa), redo it
		} else {
			iapr.VerificationInfo = ClientVerificationInfo{Products: emptyProducts, Issue: "SERVER_ERROR"}
			// Other weird error, should we do something?
		}

		return
	}

	// Check if bundle IDs are correct
	if iapr.Receipt.BundleID != "com.christianselig.Apollo" {
		// ❌ CAN REMOVE USER FROM SERVER
		iapr.VerificationInfo = ClientVerificationInfo{Products: emptyProducts, Issue: "INVALID_RECEIPT"}
		iapr.DeleteDevice = true
		return
	}

	isLifetime := iapr.hasLifetimeSubscription()
	currentTimedSubscription := iapr.currentlyActiveTimedSubscription()
	proStatus := iapr.hasNormalInAppPurchase("apollo_pro")
	communityIconsStatus := iapr.hasNormalInAppPurchase("community_icon_pack")
	spcaStatus := iapr.hasNormalInAppPurchase("com.christianselig.spcaicon")

	// For sandbox environment, be more lenient (just ensure bundle ID is accurate) because otherwise you'll break
	// things for TestFlight users (see: https://twitter.com/ChristianSelig/status/1414990459861098496)
	// TODO(andremedeiros): let this through for now
	if iapr.Environment == Sandbox && true {
		ultraProduct := VerificationProduct{Name: "ultra", Status: "SANDBOX", SubscriptionType: "SANDBOX"}
		proProduct := VerificationProduct{Name: "pro", Status: "SANDBOX"}
		communityIconsProduct := VerificationProduct{Name: "community_icons", Status: "SANDBOX"}
		spcaProduct := VerificationProduct{Name: "spca", Status: "SANDBOX"}

		products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
		iapr.VerificationInfo = ClientVerificationInfo{Products: products}
		return
	}

	proProduct := VerificationProduct{Name: "pro", Status: inAppPurchaseStatusFromCode(proStatus)}
	communityIconsProduct := VerificationProduct{Name: "community_icons", Status: inAppPurchaseStatusFromCode(communityIconsStatus)}
	spcaProduct := VerificationProduct{Name: "spca", Status: inAppPurchaseStatusFromCode(spcaStatus)}

	if isLifetime == 1 {
		if currentTimedSubscription == SubscriptionMonthly || currentTimedSubscription == SubscriptionTriMonthly || currentTimedSubscription == SubscriptionYearly {
			ultraProduct := VerificationProduct{Name: "ultra", Status: "LIFETIME_SUB_STILL_ACTIVE", SubscriptionType: "LIFETIME"}
			products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
			iapr.VerificationInfo = ClientVerificationInfo{Products: products}
		} else {
			ultraProduct := VerificationProduct{Name: "ultra", Status: "LIFETIME", SubscriptionType: "LIFETIME"}
			products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
			iapr.VerificationInfo = ClientVerificationInfo{Products: products}
		}

		return
	} else if isLifetime == 2 && currentTimedSubscription == "" {
		// ❌ CAN REMOVE USER FROM SERVER
		ultraProduct := VerificationProduct{Name: "ultra", Status: "REFUND", SubscriptionType: "LIFETIME"}
		products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
		iapr.VerificationInfo = ClientVerificationInfo{Products: products}
		iapr.DeleteDevice = true
		return
	}

	// The receipt IS valid, now do a bit more digging to ensure further
	if len(iapr.LatestReceiptInfo) > 0 && currentTimedSubscription != "" {
		mostRecentTransactionIndex := 0
		mostRecentTransactionTime := int64(0)
		choseOne := false

		// We do things a little funny. If we just grab the furthest date in the future, we could (it happened)
		// prevent someone who cancelled a yearly subscription, then renewed with a monthly subscription, from
		// being labeled a valid user as we just grabbed the furthest date. So grab the furthest date that isn't cancelled.
		for index, transaction := range iapr.LatestReceiptInfo {
			if transaction.ExpiresDateMS > mostRecentTransactionTime && transaction.CancellationReason == "" {
				mostRecentTransactionIndex = index
				mostRecentTransactionTime = transaction.ExpiresDateMS
				choseOne = true
			}
		}

		// BUT, in case there is no such date (they cancelled) we want to be able to communicate that to them, so
		// if we didn't select any, use the old method where it'll find a cancelled one to communicate back
		if !choseOne {
			for index, transaction := range iapr.LatestReceiptInfo {
				if transaction.ExpiresDateMS > mostRecentTransactionTime {
					mostRecentTransactionIndex = index
					mostRecentTransactionTime = transaction.ExpiresDateMS
				}
			}
		}

		mostRecentTransaction := iapr.LatestReceiptInfo[mostRecentTransactionIndex]

		// Check if product IDs are correct
		if !strings.HasPrefix(mostRecentTransaction.ProductID, "com.christianselig.apollo.sub") {
			// ❌ CAN REMOVE USER FROM SERVER
			iapr.VerificationInfo = ClientVerificationInfo{Products: emptyProducts, Issue: "INVALID_RECEIPT"}
			iapr.DeleteDevice = true
			return
		}

		// Check if Apple Customer Service cancelled subscription for user (and why)
		if mostRecentTransaction.CancellationReason == "0" || mostRecentTransaction.CancellationReason == "1" {
			// ❌ CAN REMOVE USER FROM SERVER
			ultraProduct := VerificationProduct{Name: "ultra", Status: "REFUND", SubscriptionType: currentTimedSubscription}
			products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
			iapr.VerificationInfo = ClientVerificationInfo{Products: products}
			iapr.DeleteDevice = true
			return
		}

		// Comes in milliseconds, convert to normal seconds
		mostRecentTransactionUnixTimestamp := mostRecentTransaction.ExpiresDateMS / 1000

		// Check if it's not active
		currentTimeUnixTimestamp := int64(time.Now().Unix())

		if mostRecentTransactionUnixTimestamp < currentTimeUnixTimestamp {
			if len(iapr.PendingRenewalInfo) > 0 && iapr.PendingRenewalInfo[0].SubscriptionAutoRenewStatus == "0" {
				// Expired and user disabled auto-renew
				// ❌ CAN REMOVE USER FROM SERVER
				ultraProduct := VerificationProduct{Name: "ultra", Status: "INACTIVE_DID_NOT_RENEW", SubscriptionType: currentTimedSubscription}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
				iapr.DeleteDevice = true
				return
			}

			if len(iapr.PendingRenewalInfo) > 0 && iapr.PendingRenewalInfo[0].SubscriptionRetryFlag == "1" {
				// Apple is still trying to rebill, so consider this their grace period
				// Note: this also encompasses the official Apple "grace period" feature that Apollo enabled, but as it's only 16 days and the billing retry period is 60, our leniency with the billing retry period fully encompasses the grace period as well
				ultraProduct := VerificationProduct{Name: "ultra", Status: "ACTIVE_GRACE_PERIOD", SubscriptionType: currentTimedSubscription}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
			} else {
				// Billing retry period is over, so subscription is inactive due to a billing issue
				// ❌ CAN REMOVE USER FROM SERVER
				ultraProduct := VerificationProduct{Name: "ultra", Status: "INACTIVE_BILLING_ISSUE", SubscriptionType: currentTimedSubscription}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
				iapr.DeleteDevice = true
			}

			return
		}

		// We've passed all the checks, return a thumbs up
		if len(iapr.PendingRenewalInfo) > 0 && iapr.PendingRenewalInfo[0].SubscriptionAutoRenewStatus == "1" {
			// They're auto-renewing! Indicate this
			ultraProduct := VerificationProduct{Name: "ultra", Status: "ACTIVE_AUTORENEW_ON", SubscriptionType: currentTimedSubscription}
			products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
			iapr.VerificationInfo = ClientVerificationInfo{Products: products}
			return
		} else {
			// They're NOT auto renewing
			// If they're within 8 days of it expiring because of this, indicate so
			eightDaysInSeconds := int64(60 * 60 * 24 * 8)
			if mostRecentTransactionUnixTimestamp-currentTimeUnixTimestamp < eightDaysInSeconds {
				ultraProduct := VerificationProduct{Name: "ultra", Status: "ACTIVE_AUTORENEW_OFF_CLOSE_EXPIRY", SubscriptionType: currentTimedSubscription}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
			} else {
				ultraProduct := VerificationProduct{Name: "ultra", Status: "ACTIVE_AUTORENEW_OFF_DISTANT_EXPIRY", SubscriptionType: currentTimedSubscription}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
			}

			return
		}
	} else {
		// We get here if they have no currently active timed subscription (ie: monthly, annually) AND no lifetime unlock
		if len(iapr.PendingRenewalInfo) > 0 && iapr.PendingRenewalInfo[0].SubscriptionExpirationIntent != "" {
			// If expiration intent has a value, it means the user let their subscription not renew and it's properly expired.
			// Since the previous `currentlyActiveTimedSubscription` function also found no active other subscriptions, it
			// stands to reason that there are also no other valid pending subscriptions, so we can safely say they expired
			if iapr.PendingRenewalInfo[0].SubscriptionExpirationIntent == "2" {
				// Billing issue
				// ❌ CAN REMOVE USER FROM SERVER
				ultraProduct := VerificationProduct{Name: "ultra", Status: "INACTIVE_BILLING_ISSUE"}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
				iapr.DeleteDevice = true
			} else {
				// Cancelled for some other reason
				// ❌ CAN REMOVE USER FROM SERVER
				ultraProduct := VerificationProduct{Name: "ultra", Status: "INACTIVE_DID_NOT_RENEW"}
				products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
				iapr.VerificationInfo = ClientVerificationInfo{Products: products}
				iapr.DeleteDevice = true
			}

			return
		} else {
			// ❌ CAN REMOVE USER FROM SERVER
			ultraProduct := VerificationProduct{Name: "ultra", Status: "NO"}
			products := []VerificationProduct{ultraProduct, proProduct, communityIconsProduct, spcaProduct}
			iapr.VerificationInfo = ClientVerificationInfo{Products: products}
			iapr.DeleteDevice = true
			return
		}
	}
}

func inAppPurchaseStatusFromCode(code int) string {
	if code == 0 {
		return "NO"
	} else if code == 1 {
		return "LIFETIME"
	} else {
		return "REFUND"
	}
}

func (iapr *IAPResponse) hasNormalInAppPurchase(prefix string) int {
	// Returns 0 if false, 1 if true, 2 if false because cancelled by customer service

	// Check through all of them, in two stages, because they might have refunded Pro but bought it again later, so look for at least one
	for _, transaction := range iapr.LatestReceiptInfo {
		if strings.HasPrefix(transaction.ProductID, prefix) && transaction.CancellationReason == "" {
			return 1
		}
	}

	// If we got here, there's no non-cancelled Pro purchases on the receipt, so now check if there's any cancelled ones and return if so
	for _, transaction := range iapr.LatestReceiptInfo {
		if strings.HasPrefix(transaction.ProductID, prefix) {
			if transaction.CancellationReason != "" {
				return 2
			}
		}
	}

	return 0
}

func (iapr *IAPResponse) hasLifetimeSubscription() int {
	// return 0 if true, 1 if false, 2 if false because it was cancelled by customer service
	// return 0 if false, 1 if true, 2 if false because it was cancelled by customer service
	// -1 is unknown (beginning value)
	var tentativeValue = -1

	for _, transaction := range iapr.LatestReceiptInfo {
		if transaction.ProductID == "com.christianselig.apollo.ultra.lifetime" {
			if transaction.CancellationReason == "0" || transaction.CancellationReason == "1" {
				// Protect against the case that they have one Ultra purchase refunded, but another one that wasn't, we don't want the first refund to negate the fact they legitimately bought it the second time
				if tentativeValue != 1 {
					tentativeValue = 2
				}
			} else {
				tentativeValue = 1
			}
		}
	}

	for _, transaction := range iapr.Receipt.InApp {
		if transaction.ProductID == "com.christianselig.apollo.ultra.lifetime" {
			if transaction.CancellationReason == "0" || transaction.CancellationReason == "1" {
				if tentativeValue != 1 {
					tentativeValue = 2
				}
			} else {
				tentativeValue = 1
			}
		}
	}

	if tentativeValue == -1 {
		return 0
	} else {
		return tentativeValue
	}
}

func (iapr *IAPResponse) currentlyActiveTimedSubscription() string {
	if len(iapr.PendingRenewalInfo) == 0 {
		return ""
	}

	timedStatus := ""

	for _, info := range iapr.PendingRenewalInfo {
		if info.SubscriptionExpirationIntent != "" || info.SubscriptionAutoRenewStatus == "0" {
			timedStatus = ""
		} else if info.SubscriptionAutoRenewProductID == "com.christianselig.apollo.sub.monthly" {
			timedStatus = SubscriptionMonthly
			break
		} else if info.SubscriptionAutoRenewProductID == "com.christianselig.apollo.sub.yearly" {
			timedStatus = SubscriptionYearly
			break
		}
	}

	return timedStatus
}
