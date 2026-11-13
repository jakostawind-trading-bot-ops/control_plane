package repositories

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

var (
	VALKEY_CLIENT_NOT_INITIATED = errors.New("SessionsValkeyRepo: Valkey client is not initialized")
	INVALID_DATA_OR_TTL         = errors.New("SessionsValkeyRepo: invalid session data or TTL")
	SESSION_NOT_SAVED           = errors.New("SessionsValkeyRepo: session not saved")
)

// SessionsValkeyRepo сохраняет сессии в Valkey.
// DI устанавливает Client при запуске, до начала обработки HTTP-запросов.
type SessionsValkeyRepo struct {
	Logger *slog.Logger  // Логгер, передаваемый DI; обязателен.
	Client valkey.Client // Подключённый клиент; временем жизни управляет DI.
}

var _ SessionsRepo = (*SessionsValkeyRepo)(nil)

// Save сохраняет имя пользователя под ключом с хешем токена.
// ctx связывает span с вызывающей операцией и отменяет запрос к Valkey.
// token и username должны быть непустыми;
// ttl задаёт положительный срок жизни в целых секундах.
// Возвращает ошибку при неверных аргументах, отсутствии клиента,
// существующей сессии с тем же токеном или сбое Valkey.
//
// Этапы:
//  1. Проверить зависимости и аргументы.
//  2. Вычислить ключ и атомарно сохранить сессию с TTL.
//
// Схема:
//
//	token -> SHA-256 -> SET NX EX -> Valkey
func (SessionsValkeyRepo *SessionsValkeyRepo) Save(
	ctx context.Context,
	token, username string,
	ttl time.Duration,
) error {
	Logger := SessionsValkeyRepo.Logger.With("operation", "session_valkey_repo")
	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/sessions",
	)
	ctx, span := Tracer.Start(ctx, "SessionsValkeyRepo.Save")
	defer span.End()

	// Этап 1: исключаем запись некорректной сессии.
	if SessionsValkeyRepo.Client == nil {
		Logger.DebugContext(
			ctx, "Клиент Valkey не инициалзирован",
			"error", VALKEY_CLIENT_NOT_INITIATED,
		)
		span.RecordError(VALKEY_CLIENT_NOT_INITIATED)
		span.SetStatus(codes.Error, VALKEY_CLIENT_NOT_INITIATED.Error())
		return VALKEY_CLIENT_NOT_INITIATED
	}
	if token == "" || username == "" ||
		ttl <= 0 || ttl%time.Second != 0 {
		Logger.DebugContext(
			ctx, "Н еверный токен или юзернейм",
			"error", INVALID_DATA_OR_TTL,
		)
		span.RecordError(INVALID_DATA_OR_TTL)
		span.SetStatus(codes.Error, INVALID_DATA_OR_TTL.Error())
		return INVALID_DATA_OR_TTL
	}

	// Этап 2: исходный токен не сохраняется; TTL назначается при записи.
	hash := sha256.Sum256([]byte(token))
	key := "auth:session:" + hex.EncodeToString(hash[:])

	err := SessionsValkeyRepo.Client.Do(ctx, SessionsValkeyRepo.Client.B().
		Set().
		Key(key).
		Value(username).
		Nx().
		Ex(ttl).
		Build(),
	).Error()
	if err != nil {
		err = fmt.Errorf("%w: %w", SESSION_NOT_SAVED, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, SESSION_NOT_SAVED.Error())
		return err
	}

	return nil
}
