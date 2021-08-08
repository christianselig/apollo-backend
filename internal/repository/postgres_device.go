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
			&dev.LastPingedAt,
		); err != nil {
			return nil, err
		}
		devs = append(devs, dev)
	}
	return devs, nil
}

func (p *postgresDeviceRepository) GetByAPNSToken(ctx context.Context, token string) (domain.Device, error) {
	query := `
		SELECT id, apns_token, sandbox, last_pinged_at
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

func (p *postgresDeviceRepository) CreateOrUpdate(ctx context.Context, dev *domain.Device) error {
	fmt.Println(dev)
	query := `
		INSERT INTO devices (apns_token, sandbox, last_pinged_at)
		VALUES ($1, $2, $3)
		ON CONFLICT(apns_token) DO
			UPDATE SET last_pinged_at = $3
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		dev.APNSToken,
		dev.Sandbox,
		dev.LastPingedAt,
	).Scan(&dev.ID)
}

func (p *postgresDeviceRepository) Create(ctx context.Context, dev *domain.Device) error {
	query := `
		INSERT INTO devices
			(apns_token, sandbox, last_pinged_at)
		VALUES ($1, $2, $3)
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		dev.APNSToken,
		dev.Sandbox,
		dev.LastPingedAt,
	).Scan(&dev.ID)
}

func (p *postgresDeviceRepository) Update(ctx context.Context, dev *domain.Device) error {
	query := `
		UPDATE devices
		SET last_pinged_at = $2
		WHERE id = $1`

	res, err := p.pool.Exec(ctx, query, dev.ID, dev.LastPingedAt)

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
