package repositories

import (
	"context"
	"errors"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

var (
	CONTROL_USER_NOT_FOUND = errors.New("UsersBootstrapSettingsRepo: control user not found")
	INVALID_DATA           = errors.New("UsersBootstrapSettingsRepo: invalid username or password")
)

// UsersBootstrapSettingsRepo читает учётные данные из настроек запуска.
type UsersBootstrapSettingsRepo struct {
	BootstrapSettings *internal.BootstrapSettings // Источник учётных данных; nil приводит к INVALID_DATA.
}

// GetUser возвращает учётные данные управляющего пользователя.
// ctx задаёт родительский span; чтение настроек выполняется синхронно.
// При отсутствии настроек, имени или пароля возвращает пустую сущность
// и INVALID_DATA; иначе возвращает пользователя и nil.
func (UsersBootstrapSettingsRepo *UsersBootstrapSettingsRepo) GetUser(ctx context.Context) (entities.UserEntity, error) {
	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/users",
	)
	_, span := Tracer.Start(ctx, "UsersBootstrapSettingsRepo.GetUser")
	defer span.End()

	if UsersBootstrapSettingsRepo.BootstrapSettings == nil ||
		UsersBootstrapSettingsRepo.BootstrapSettings.ControlUsername == "" ||
		UsersBootstrapSettingsRepo.BootstrapSettings.ControlPassword == "" {
		span.RecordError(INVALID_DATA)
		span.SetStatus(codes.Error, INVALID_DATA.Error())
		return entities.UserEntity{}, INVALID_DATA
	}

	return entities.UserEntity{
		Username: UsersBootstrapSettingsRepo.BootstrapSettings.ControlUsername,
		Password: UsersBootstrapSettingsRepo.BootstrapSettings.ControlPassword,
	}, nil
}
