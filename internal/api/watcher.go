package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/gorilla/mux"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/reddit"
)

type watcherCriteria struct {
	Author    string
	Subreddit string
	Upvotes   int64
	Keyword   string
	Flair     string
	Domain    string
}

type createWatcherRequest struct {
	Type      string
	User      string
	Subreddit string
	Label     string
	Criteria  watcherCriteria
}

func (cwr *createWatcherRequest) Validate() error {
	return validation.ValidateStruct(cwr,
		validation.Field(&cwr.Type, validation.Required),
		validation.Field(&cwr.User, validation.Required.When(cwr.Type == "user")),
		validation.Field(&cwr.Subreddit, validation.Required.When(cwr.Type == "subreddit" || cwr.Type == "trending")),
	)
}

type watcherCreatedResponse struct {
	ID int64 `json:"id"`
}

func (a *api) createWatcherHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)
	apns := vars["apns"]
	redditID := vars["redditID"]

	cwr := &createWatcherRequest{
		Criteria: watcherCriteria{},
	}
	if err := json.NewDecoder(r.Body).Decode(cwr); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	if err := cwr.Validate(); err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	accs, err := a.accountRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	if len(accs) == 0 {
		err := errors.New("cannot create watchers without account")
		a.errorResponse(w, r, 422, err)
		return
	}

	account := accs[0]
	found := false
	for _, acc := range accs {
		if acc.AccountID == redditID {
			found = true
			account = acc
		}
	}

	if !found {
		err := errors.New("account not associated with device")
		a.errorResponse(w, r, 401, err)
		return
	}

	watcher := domain.Watcher{
		Label:     cwr.Label,
		DeviceID:  dev.ID,
		AccountID: account.ID,
		Author:    strings.ToLower(cwr.Criteria.Author),
		Subreddit: strings.ToLower(cwr.Criteria.Subreddit),
		Upvotes:   cwr.Criteria.Upvotes,
		Keyword:   strings.ToLower(cwr.Criteria.Keyword),
		Flair:     strings.ToLower(cwr.Criteria.Flair),
		Domain:    strings.ToLower(cwr.Criteria.Domain),
	}

	if cwr.Type == "subreddit" || cwr.Type == "trending" {
		ac := a.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)
		srr, err := ac.SubredditAbout(ctx, cwr.Subreddit)
		if err != nil {
			a.errorResponse(w, r, 500, err)
			return
		}
		if !srr.Public {
			a.errorResponse(w, r, 403, reddit.ErrSubredditIsPrivate)
			return
		} else if err != nil {
			switch err {
			case reddit.ErrSubredditIsPrivate, reddit.ErrSubredditIsQuarantined:
				err = fmt.Errorf("error watching %s: %w", cwr.Subreddit, err)
				a.errorResponse(w, r, 403, err)
			default:
				a.errorResponse(w, r, 422, err)
			}
			return
		}

		sr, err := a.subredditRepo.GetByName(ctx, cwr.Subreddit)
		if err != nil {
			switch err {
			case domain.ErrNotFound:
				// Might be that we don't know about that subreddit yet
				sr = domain.Subreddit{SubredditID: srr.ID, Name: srr.Name}
				_ = a.subredditRepo.CreateOrUpdate(ctx, &sr)
			default:
				a.errorResponse(w, r, 500, err)
				return
			}
		}

		switch cwr.Type {
		case "subreddit":
			watcher.Type = domain.SubredditWatcher
		case "trending":
			watcher.Label = "trending"
			watcher.Type = domain.TrendingWatcher
		}

		watcher.WatcheeID = sr.ID
	} else if cwr.Type == "user" {
		ac := a.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)
		urr, err := ac.UserAbout(ctx, cwr.User)
		if err != nil {
			a.errorResponse(w, r, 500, err)
			return
		}

		if !urr.AcceptFollowers {
			err := errors.New("user has followers disabled")
			a.errorResponse(w, r, 403, err)
			return
		}

		u := domain.User{UserID: urr.ID, Name: urr.Name}
		err = a.userRepo.CreateOrUpdate(ctx, &u)

		if err != nil {
			a.errorResponse(w, r, 500, err)
			return
		}

		watcher.Type = domain.UserWatcher
		watcher.WatcheeID = u.ID
	} else {
		err := fmt.Errorf("unknown watcher type: %s", cwr.Type)
		a.errorResponse(w, r, 422, err)
		return
	}

	if err := a.watcherRepo.Create(ctx, &watcher); err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(watcherCreatedResponse{ID: watcher.ID})
}

func (a *api) editWatcherHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)
	apns := vars["apns"]
	wid := vars["watcherID"]
	rid := vars["redditID"]

	id, err := strconv.ParseInt(wid, 10, 64)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	watcher, err := a.watcherRepo.GetByID(ctx, id)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	} else if watcher.Device.APNSToken != vars["apns"] {
		err := fmt.Errorf("wrong device for watcher %d", watcher.ID)
		a.errorResponse(w, r, 422, err)
		return
	}

	ewr := &createWatcherRequest{
		Criteria: watcherCriteria{},
	}

	if err := json.NewDecoder(r.Body).Decode(ewr); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	watcher.Label = ewr.Label
	watcher.Author = strings.ToLower(ewr.User)
	watcher.Subreddit = strings.ToLower(ewr.Subreddit)
	watcher.Upvotes = ewr.Criteria.Upvotes
	watcher.Keyword = strings.ToLower(ewr.Criteria.Keyword)
	watcher.Flair = strings.ToLower(ewr.Criteria.Flair)
	watcher.Domain = strings.ToLower(ewr.Criteria.Domain)

	if watcher.Type == domain.SubredditWatcher {
		lsr := strings.ToLower(watcher.Subreddit)
		if watcher.WatcheeLabel != lsr {
			var account domain.Account
			accs, err := a.accountRepo.GetByAPNSToken(ctx, apns)
			if err != nil {
				a.errorResponse(w, r, 422, err)
				return
			}

			if len(accs) == 0 {
				err := errors.New("cannot create watchers without account")
				a.errorResponse(w, r, 422, err)
				return
			}

			found := false
			for _, acc := range accs {
				if acc.AccountID == rid {
					account = acc
					found = true
				}
			}

			if !found {
				err := errors.New("account not associated with device")
				a.errorResponse(w, r, 401, err)
				return
			}

			ac := a.reddit.NewAuthenticatedClient(account.AccountID, account.RefreshToken, account.AccessToken)
			srr, err := ac.SubredditAbout(ctx, lsr)
			if !srr.Public {
				a.errorResponse(w, r, 403, reddit.ErrSubredditIsPrivate)
				return
			} else if err != nil {
				switch err {
				case reddit.ErrSubredditIsPrivate, reddit.ErrSubredditIsQuarantined:
					err = fmt.Errorf("error watching %s: %w", lsr, err)
					a.errorResponse(w, r, 403, err)
				default:
					a.errorResponse(w, r, 422, err)
				}
				return
			}

			sr, err := a.subredditRepo.GetByName(ctx, lsr)
			if err != nil {
				switch err {
				case domain.ErrNotFound:
					// Might be that we don't know about that subreddit yet
					sr = domain.Subreddit{SubredditID: srr.ID, Name: srr.Name}
					_ = a.subredditRepo.CreateOrUpdate(ctx, &sr)
				default:
					a.errorResponse(w, r, 500, err)
					return
				}
			}

			watcher.WatcheeID = sr.ID
		}
	}

	if err := a.watcherRepo.Update(ctx, &watcher); err != nil {
		a.errorResponse(w, r, 500, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) deleteWatcherHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["watcherID"], 10, 64)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	}

	watcher, err := a.watcherRepo.GetByID(ctx, id)
	if err != nil {
		a.errorResponse(w, r, 422, err)
		return
	} else if watcher.Device.APNSToken != vars["apns"] {
		err := fmt.Errorf("wrong device for watcher %d", watcher.ID)
		a.errorResponse(w, r, 422, err)
		return
	}

	_ = a.watcherRepo.Delete(ctx, id)
	w.WriteHeader(http.StatusOK)
}

type watcherItem struct {
	ID          int64     `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	Type        string    `json:"type"`
	Label       string    `json:"label"`
	SourceLabel string    `json:"source_label"`
	Upvotes     int64     `json:"upvotes,omitempty"`
	Keyword     string    `json:"keyword,omitempty"`
	Flair       string    `json:"flair,omitempty"`
	Domain      string    `json:"domain,omitempty"`
	Hits        int64     `json:"hits"`
	Author      string    `json:"author,omitempty"`
}

func (a *api) listWatchersHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	apns := vars["apns"]
	redditID := vars["redditID"]

	watchers, err := a.watcherRepo.GetByDeviceAPNSTokenAndAccountRedditID(ctx, apns, redditID)
	if err != nil {
		a.errorResponse(w, r, 400, err)
		return
	}

	wis := make([]watcherItem, len(watchers))
	for i, watcher := range watchers {
		wi := watcherItem{
			ID:          watcher.ID,
			CreatedAt:   watcher.CreatedAt,
			Type:        watcher.Type.String(),
			Label:       watcher.Label,
			SourceLabel: watcher.WatcheeLabel,
			Keyword:     watcher.Keyword,
			Flair:       watcher.Flair,
			Domain:      watcher.Domain,
			Hits:        watcher.Hits,
			Author:      watcher.Author,
			Upvotes:     watcher.Upvotes,
		}

		wis[i] = wi
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(wis)
}
