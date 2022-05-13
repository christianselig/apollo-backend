package domain

import (
	"context"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const UserRefreshInterval = 2 * time.Minute

type User struct {
	ID          int64
	NextCheckAt time.Time

	// Reddit information
	UserID string
	Name   string
}

func (u *User) NormalizedName() string {
	return strings.ToLower(u.Name)
}

func (u *User) Validate() error {
	return validation.ValidateStruct(u,
		validation.Field(&u.Name, validation.Required, validation.Length(3, 20)),
		validation.Field(&u.UserID, validation.Required, validation.Length(4, 9)),
	)
}

type UserRepository interface {
	GetByID(context.Context, int64) (User, error)
	GetByName(context.Context, string) (User, error)

	CreateOrUpdate(context.Context, *User) error
	Delete(context.Context, int64) error
}
