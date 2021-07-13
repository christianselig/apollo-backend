package data

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	ErrRecordNotFound = errors.New("record not found")
)

type Models struct {
	Accounts interface {
		Upsert(a *Account) error
		Delete(id int64) error
	}

	Devices interface {
		Upsert(*Device) error
		GetByAPNSToken(string) (*Device, error)
	}

	DevicesAccounts interface {
		Associate(int64, int64) error
	}
}

func NewModels(ctx context.Context, pool *pgxpool.Pool) *Models {
	return &Models{
		Accounts:        &AccountModel{ctx, pool},
		Devices:         &DeviceModel{ctx, pool},
		DevicesAccounts: &DeviceAccountModel{ctx, pool},
	}
}

func NewMockModels() *Models {
	return &Models{
		Accounts:        &MockAccountModel{},
		Devices:         &MockDeviceModel{},
		DevicesAccounts: &MockDeviceAccountModel{},
	}
}
