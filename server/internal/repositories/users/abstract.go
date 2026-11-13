package repositories

import (
	"context"

	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
)

type UsersRepo interface {
	GetUser(ctx context.Context) (entities.UserEntity, error)
}
