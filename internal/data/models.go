package data

import (
	"database/sql"
	"errors"
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

func NewModels(db *sql.DB) *Models {
	return &Models{
		Accounts:        &AccountModel{DB: db},
		Devices:         &DeviceModel{DB: db},
		DevicesAccounts: &DeviceAccountModel{DB: db},
	}
}

func NewMockModels() *Models {
	return &Models{
		Accounts:        &MockAccountModel{},
		Devices:         &MockDeviceModel{},
		DevicesAccounts: &MockDeviceAccountModel{},
	}
}
