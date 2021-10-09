package domain

import (
	"context"
	"strings"
)

type User struct {
	ID            int64
	LastCheckedAt float64

	// Reddit information
	UserID string
	Name   string
}

func (u *User) NormalizedName() string {
	return strings.ToLower(u.Name)
}

type UserRepository interface {
	GetByID(context.Context, int64) (User, error)
	GetByName(context.Context, string) (User, error)

	CreateOrUpdate(context.Context, *User) error
	Delete(context.Context, int64) error
}
