package repositories

import (
	"context"
	"errors"

	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
)

var (
	CONTROL_USER_NOT_FOUND = errors.New("UserRepo: control user not found")
	INVALID_DATA = errors.New("UserRepo: invalid username or password")
)

type UsersRepo interface {
	GetUser(ctx context.Context) (entities.UserEntity, error) 
}
