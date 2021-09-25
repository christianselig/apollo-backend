package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/gorilla/mux"
)

type watcherCriteria struct {
	Upvotes int64
	Keyword string
	Flair   string
	Domain  string
}

type createWatcherRequest struct {
	Subreddit string
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

	watcher := domain.Watcher{
		SubredditID: sr.ID,
		DeviceID:    dev.ID,
		AccountID:   account.ID,
		Upvotes:     cwr.Criteria.Upvotes,
		Keyword:     cwr.Criteria.Keyword,
		Flair:       cwr.Criteria.Flair,
		Domain:      cwr.Criteria.Domain,
	}

	if err := a.watcherRepo.Create(ctx, &watcher); err != nil {
		a.errorResponse(w, r, 422, err.Error())
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

	apns := vars["apns"]

	dev, err := a.deviceRepo.GetByAPNSToken(ctx, apns)
	if err != nil {
		a.errorResponse(w, r, 422, err.Error())
		return
	}

	watcher, err := a.watcherRepo.GetByID(ctx, id)
	if err != nil || watcher.DeviceID != dev.ID {
		a.errorResponse(w, r, 422, "nice try")
		return
	}

	_ = a.watcherRepo.Delete(ctx, id)
	w.WriteHeader(http.StatusOK)
}

type watcherItem struct {
	ID      int64
	Upvotes int64
	Keyword string
	Flair   string
	Domain  string
	Hits    int64
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
			ID:      watcher.ID,
			Upvotes: watcher.Upvotes,
			Keyword: watcher.Keyword,
			Flair:   watcher.Flair,
			Domain:  watcher.Domain,
			Hits:    watcher.Hits,
		}

		wis[i] = wi
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(wis)
}
