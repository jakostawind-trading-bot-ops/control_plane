package auth_usecases

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
	sessions_repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/sessions"
	repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/users"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

// SessionTTL ограничивает срок жизни сессии; HTTP использует его для cookie.
const SessionTTL = 24 * time.Hour

var (
	INVALID_CREDENTIALS         = errors.New("LoginUC: invalid username or password")
	USER_NOT_LOADED             = errors.New("LoginUC: load user")
	SESSION_TOKEN_NOT_GENERATED = errors.New("LoginUC: generate session token")
	SESSION_NOT_SAVED           = errors.New("LoginUC: save session")
)

// LoginUC проверяет учётные данные и выдаёт сохранённую сессию.
type LoginUC struct {
	Logger       *slog.Logger                       // Логгер без записи паролей и токенов.
	UserRepo     repositories.UsersRepo             // Источник учётных данных.
	SessionsRepo sessions_repositories.SessionsRepo // Хранилище сессий с TTL.
}

// Execute проверяет RequestUser и возвращает секретный токен новой сессии.
// ctx передаёт репозиториям отмену и span сценария как родительский.
// RequestUser содержит введённые имя пользователя и пароль.
// При ошибке проверки, генерации или сохранения возвращает пустой токен и error.
// Ошибки загрузки, генерации и сохранения оборачиваются маркерами этапов,
// сохраняя исходную причину для errors.Is и errors.As.
//
// Этапы:
//  1. Получить пользователя и проверить учётные данные.
//  2. Сгенерировать криптографически случайный токен.
//  3. Сохранить сессию и только после этого вернуть токен.
//
// Схема:
//
//	RequestUser -> UsersRepo -> проверка -> токен -> SessionsRepo -> результат
//	                  +-- ошибка на любом этапе -> пустой токен, error
func (LoginUC LoginUC) Execute(
	ctx context.Context,
	RequestUser entities.UserEntity,
) (string, error) {
	Logger := LoginUC.Logger.With("operation", "login_use_case")
	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth",
	)
	ctx, span := Tracer.Start(ctx, "LoginUC.Execute")
	defer span.End()

	// Этап 1: до успешной проверки сессия не создаётся.
	User, err := LoginUC.UserRepo.GetUser(ctx)
	if err != nil {
		err = fmt.Errorf("%w: %w", USER_NOT_LOADED, err)
		Logger.DebugContext(
			ctx, "Ошибка UserRepo",
			"error", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, USER_NOT_LOADED.Error())
		return "", err
	}

	if RequestUser.Username != User.Username ||
		RequestUser.Password != User.Password {
		Logger.DebugContext(ctx, "Неверный юзернейм или пароль")
		span.RecordError(INVALID_CREDENTIALS)
		span.SetStatus(codes.Error, INVALID_CREDENTIALS.Error())
		return "", INVALID_CREDENTIALS
	}

	// Этап 2: создаём 256-битный токен для передачи клиенту.
	var TokenBytes [32]byte
	if _, err := rand.Read(TokenBytes[:]); err != nil {
		err = fmt.Errorf("%w: %w", SESSION_TOKEN_NOT_GENERATED, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, SESSION_TOKEN_NOT_GENERATED.Error())
		return "", err
	}
	Token := base64.RawURLEncoding.EncodeToString(TokenBytes[:])

	// Этап 3: ошибка сохранения исключает успешную выдачу токена.
	if err := LoginUC.SessionsRepo.Save(ctx, Token, User.Username, SessionTTL); err != nil {
		err = fmt.Errorf("%w: %w", SESSION_NOT_SAVED, err)
		Logger.ErrorContext(ctx, "Ошибка сохранения сессии", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, SESSION_NOT_SAVED.Error())
		return "", err
	}

	Logger.InfoContext(ctx, "LoginUC выполнился")
	return Token, nil
}
