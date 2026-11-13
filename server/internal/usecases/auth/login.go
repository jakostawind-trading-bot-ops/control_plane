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

// sessionTTL ограничивает срок жизни новой сессии с момента записи в Valkey.
const sessionTTL = 24 * time.Hour

var (
	INVALID_CREDENTIALS = errors.New("LoginUC: invalid username or password")
)

// LoginUC проверяет учётные данные и выдаёт сохранённую сессию.
type LoginUC struct {
	Logger       *slog.Logger                       // Логгер без записи паролей и токенов.
	UserRepo     repositories.UsersRepo             // Источник учётных данных.
	SessionsRepo sessions_repositories.SessionsRepo // Хранилище сессий с TTL.
}

// Execute проверяет RequestUser и возвращает секретный токен новой сессии.
// ctx передаётся репозиториям для отмены операций.
// RequestUser содержит введённые имя пользователя и пароль.
// При ошибке проверки, генерации или сохранения возвращает пустой токен и error.
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
		"github.com/jakostawind-trading-bot-ops/control_plane/server/internal/usecases/auth/login.go",
	)
	ctx, span := Tracer.Start(ctx, "LoginUC.Execute")
	defer span.End()

	// Этап 1: до успешной проверки сессия не создаётся.
	User, err := LoginUC.UserRepo.GetUser(ctx)
	if err != nil {
		LoginUC.Logger.DebugContext(
			ctx, "Ошибка UserRepo",
			"error", err,
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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
		err = fmt.Errorf("LoginUC: generate session token: %w", err) //!
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	Token := base64.RawURLEncoding.EncodeToString(TokenBytes[:])

	// Этап 3: ошибка сохранения исключает успешную выдачу токена.
	if err := LoginUC.SessionsRepo.Save(ctx, Token, User.Username, sessionTTL); err != nil {
		Logger.ErrorContext(ctx, "Ошибка сохранения сессии", "error", err) // !
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	Logger.InfoContext(
		ctx, "LoginUC выполнился",
		"Token", Token,
	)
	return Token, nil
}
