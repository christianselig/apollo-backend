package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/gorilla/mux"
)

type watcherCriteria struct {
	Author  string
	Upvotes int64
	Keyword string
	Flair   string
	Domain  string
}

type createWatcherRequest struct {
	Type      string
	User      string
	Subreddit string
	Label     string
	Criteria  watcherCriteria
}

func (a *api) createWatcherHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	vars := mux.Vars(r)
	apns := vars["apns"]
	redditID := vars["redditID"]

	cwr := &createWatcherRequest{
		Criteria: watcherCriteria{
			Upvotes: 0,
			Keyword: "",
			Flair:   "",
			Domain:  "",
		},
	}
	if err := json.NewDecoder(r.Body).Decode(cwr); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	accs, err := a.accountRepo.GetByAPNSToken(ctx, apns)
	if err != nil || len(accs) == 0 {
		a.errorResponse(w, r, 422, err.Error())
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
		a.errorResponse(w, r, 422, "yeah nice try")
		return
	}

	ac := a.reddit.NewAuthenticatedClient(account.RefreshToken, account.AccessToken)

	watcher := domain.Watcher{
		Label:     cwr.Label,
		DeviceID:  dev.ID,
		AccountID: account.ID,
		Author:    strings.ToLower(cwr.Criteria.Author),
		Upvotes:   cwr.Criteria.Upvotes,
		Keyword:   strings.ToLower(cwr.Criteria.Keyword),
		Flair:     strings.ToLower(cwr.Criteria.Flair),
		Domain:    strings.ToLower(cwr.Criteria.Domain),
	}

	if cwr.Type == "subreddit" || cwr.Type == "trending" {
		srr, err := ac.SubredditAbout(cwr.Subreddit)
		if err != nil {
			a.errorResponse(w, r, 422, err.Error())
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
				a.errorResponse(w, r, 500, err.Error())
				return
			}
		}

		switch cwr.Type {
		case "subreddit":
			watcher.Type = domain.SubredditWatcher
		case "trending":
			watcher.Type = domain.TrendingWatcher
		}

		watcher.WatcheeID = sr.ID
	} else if cwr.Type == "user" {
		urr, err := ac.UserAbout(cwr.User)
		if err != nil {
			a.errorResponse(w, r, 500, err.Error())
			return
		}

		if !urr.AcceptFollowers {
			a.errorResponse(w, r, 422, "no followers accepted")
			return
		}

		u := domain.User{UserID: urr.ID, Name: urr.Name}
		err = a.userRepo.CreateOrUpdate(ctx, &u)

		if err != nil {
			a.errorResponse(w, r, 500, err.Error())
			return
		}

		watcher.Type = domain.UserWatcher
		watcher.WatcheeID = u.ID
	} else {
		a.errorResponse(w, r, 422, "unknown watcher type")
		return
	}

	if err := a.watcherRepo.Create(ctx, &watcher); err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) editWatcherHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["watcherID"], 10, 64)
	if err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	watcher, err := a.watcherRepo.GetByID(ctx, id)
	if err != nil || watcher.Device.APNSToken != vars["apns"] {
		a.errorResponse(w, r, 422, "nice try")
		return
	}

	ewr := &createWatcherRequest{
		Criteria: watcherCriteria{
			Upvotes: 0,
			Keyword: "",
			Flair:   "",
			Domain:  "",
		},
	}

	if err := json.NewDecoder(r.Body).Decode(ewr); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	watcher.Label = ewr.Label
	watcher.Upvotes = ewr.Criteria.Upvotes
	watcher.Keyword = ewr.Criteria.Keyword
	watcher.Flair = ewr.Criteria.Flair
	watcher.Domain = ewr.Criteria.Domain

	if err := a.watcherRepo.Update(ctx, &watcher); err != nil {
		a.errorResponse(w, r, 500, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *api) deleteWatcherHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	vars := mux.Vars(r)
	id, err := strconv.ParseInt(vars["watcherID"], 10, 64)
	if err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	watcher, err := a.watcherRepo.GetByID(ctx, id)
	if err != nil || watcher.Device.APNSToken != vars["apns"] {
		a.errorResponse(w, r, 422, "nice try")
		return
	}

	_ = a.watcherRepo.Delete(ctx, id)
	w.WriteHeader(http.StatusOK)
}

type watcherItem struct {
	ID        int64
	CreatedAt float64
	Type      string
	Label     string
	Upvotes   int64
	Keyword   string
	Flair     string
	Domain    string
	Hits      int64
}

func (a *api) listWatchersHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	vars := mux.Vars(r)
	apns := vars["apns"]
	redditID := vars["redditID"]

	watchers, err := a.watcherRepo.GetByDeviceAPNSTokenAndAccountRedditID(ctx, apns, redditID)
	if err != nil {
		a.errorResponse(w, r, 400, err.Error())
		return
	}

	wis := make([]watcherItem, len(watchers))
	for i, watcher := range watchers {
		wi := watcherItem{
			ID:        watcher.ID,
			CreatedAt: watcher.CreatedAt,
			Type:      watcher.Type.String(),
			Label:     watcher.Label,
			Upvotes:   watcher.Upvotes,
			Keyword:   watcher.Keyword,
			Flair:     watcher.Flair,
			Domain:    watcher.Domain,
			Hits:      watcher.Hits,
		}

		wis[i] = wi
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(wis)
}
