package domain

import "errors"

var (
	// ErrNotFound will be returned if the requested item is not found
	ErrNotFound = errors.New("requested item was not found")
	// ErrConflict will be returned if the item being persisted already exists
	ErrConflict = errors.New("item already exists")
)
