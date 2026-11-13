package repositories

import (
	"context"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
)

type UsersBootstrapSettingsRepo struct {
    BootstrapSettings *internal.BootstrapSettings
}

func (UsersBootstrapSettingsRepo *UsersBootstrapSettingsRepo) GetUser(ctx context.Context) (entities.UserEntity, error) {
    if UsersBootstrapSettingsRepo.BootstrapSettings == nil ||
        UsersBootstrapSettingsRepo.BootstrapSettings.ControlUsername == "" ||
        UsersBootstrapSettingsRepo.BootstrapSettings.ControlPassword == "" {
        return entities.UserEntity{}, INVALID_DATA
    }

    return entities.UserEntity{
        Username: UsersBootstrapSettingsRepo.BootstrapSettings.ControlUsername,
        Password: UsersBootstrapSettingsRepo.BootstrapSettings.ControlPassword,
    }, nil
}