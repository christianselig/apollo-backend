package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresDeviceRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresDevice(pool *pgxpool.Pool) domain.DeviceRepository {
	return &postgresDeviceRepository{pool: pool}
}

func (p *postgresDeviceRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.Device, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var devs []domain.Device
	for rows.Next() {
		var dev domain.Device
		if err := rows.Scan(
			&dev.ID,
			&dev.APNSToken,
			&dev.Sandbox,
			&dev.ActiveUntil,
			&dev.GracePeriodUntil,
		); err != nil {
			return nil, err
		}
		devs = append(devs, dev)
	}
	return devs, nil
}

func (p *postgresDeviceRepository) GetByID(ctx context.Context, id int64) (domain.Device, error) {
	query := `
		SELECT id, apns_token, sandbox, active_until, grace_period_until
		FROM devices
		WHERE id = $1`

	devs, err := p.fetch(ctx, query, id)

	if err != nil {
		return domain.Device{}, err
	}
	if len(devs) == 0 {
		return domain.Device{}, domain.ErrNotFound
	}
	return devs[0], nil
}

func (p *postgresDeviceRepository) GetByAPNSToken(ctx context.Context, token string) (domain.Device, error) {
	query := `
		SELECT id, apns_token, sandbox, active_until, grace_period_until
		FROM devices
		WHERE apns_token = $1`

	devs, err := p.fetch(ctx, query, token)

	if err != nil {
		return domain.Device{}, err
	}
	if len(devs) == 0 {
		return domain.Device{}, domain.ErrNotFound
	}
	return devs[0], nil
}

func (p *postgresDeviceRepository) GetByAccountID(ctx context.Context, id int64) ([]domain.Device, error) {
	query := `
		SELECT devices.id, apns_token, sandbox, active_until, grace_period_until
		FROM devices
		INNER JOIN devices_accounts ON devices.id = devices_accounts.device_id
		WHERE devices_accounts.account_id = $1`

	return p.fetch(ctx, query, id)
}

func (p *postgresDeviceRepository) GetInboxNotifiableByAccountID(ctx context.Context, id int64) ([]domain.Device, error) {
	query := `
		SELECT devices.id, apns_token, sandbox, active_until, grace_period_until
		FROM devices
		INNER JOIN devices_accounts ON devices.id = devices_accounts.device_id
		WHERE devices_accounts.account_id = $1 AND
		devices_accounts.inbox_notifiable = TRUE AND
		grace_period_until > NOW()`

	return p.fetch(ctx, query, id)
}

func (p *postgresDeviceRepository) GetWatcherNotifiableByAccountID(ctx context.Context, id int64) ([]domain.Device, error) {
	query := `
		SELECT devices.id, apns_token, sandbox, active_until, grace_period_until
		FROM devices
		INNER JOIN devices_accounts ON devices.id = devices_accounts.device_id
		WHERE devices_accounts.account_id = $1 AND
		devices_accounts.watcher_notifiable = TRUE AND
		grace_period_until > NOW()`

	return p.fetch(ctx, query, id)
}

func (p *postgresDeviceRepository) CreateOrUpdate(ctx context.Context, dev *domain.Device) error {
	query := `
		INSERT INTO devices (apns_token, sandbox, active_until, grace_period_until)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT(apns_token) DO
			UPDATE SET active_until = $3, grace_period_until = $4
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		dev.APNSToken,
		dev.Sandbox,
		dev.ActiveUntil,
		dev.GracePeriodUntil,
	).Scan(&dev.ID)
}

func (p *postgresDeviceRepository) Create(ctx context.Context, dev *domain.Device) error {
	if err := dev.Validate(); err != nil {
		return err
	}

	query := `
		INSERT INTO devices
			(apns_token, sandbox, active_until, grace_period_until)
		VALUES ($1, $2, $3, $4)
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		dev.APNSToken,
		dev.Sandbox,
		dev.ActiveUntil,
		dev.GracePeriodUntil,
	).Scan(&dev.ID)
}

func (p *postgresDeviceRepository) Update(ctx context.Context, dev *domain.Device) error {
	if err := dev.Validate(); err != nil {
		return err
	}

	query := `
		UPDATE devices
		SET active_until = $2, grace_period_until = $3
		WHERE id = $1`

	res, err := p.pool.Exec(ctx, query, dev.ID, dev.ActiveUntil, dev.GracePeriodUntil)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}

func (p *postgresDeviceRepository) Delete(ctx context.Context, token string) error {
	query := `DELETE FROM devices WHERE apns_token = $1`

	res, err := p.pool.Exec(ctx, query, token)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}

func (p *postgresDeviceRepository) SetNotifiable(ctx context.Context, dev *domain.Device, acct *domain.Account, inbox, watcher, global bool) error {
	query := `
		UPDATE devices_accounts
		SET
			inbox_notifiable = $1,
			watcher_notifiable = $2,
			global_mute = $3
		WHERE device_id = $4 AND account_id = $5`

	res, err := p.pool.Exec(ctx, query, inbox, watcher, global, dev.ID, acct.ID)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err

}

func (p *postgresDeviceRepository) GetNotifiable(ctx context.Context, dev *domain.Device, acct *domain.Account) (bool, bool, bool, error) {
	query := `
		SELECT inbox_notifiable, watcher_notifiable, global_mute
		FROM devices_accounts
		WHERE device_id = $1 AND account_id = $2`

	rows, err := p.pool.Query(ctx, query, dev.ID, acct.ID)
	if err != nil {
		return false, false, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var inbox, watcher, global bool
		if err := rows.Scan(&inbox, &watcher, &global); err != nil {
			return false, false, false, err
		}
		return inbox, watcher, global, nil
	}

	return false, false, false, domain.ErrNotFound
}

func (p *postgresDeviceRepository) PruneStale(ctx context.Context, before int64) (int64, error) {
	query := `DELETE FROM devices WHERE grace_period_until < $1`

	res, err := p.pool.Exec(ctx, query, before)

	return res.RowsAffected(), err
}
