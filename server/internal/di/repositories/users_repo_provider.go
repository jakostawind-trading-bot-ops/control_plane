package di_repositories

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/users"
)

func UsersRepoProvider(
	BootstrapSettings *internal.BootstrapSettings,
) repositories.UsersRepo {
	return &repositories.UsersBootstrapSettingsRepo{
		BootstrapSettings: BootstrapSettings,
	}
}