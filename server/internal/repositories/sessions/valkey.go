package repositories

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// SessionsValkeyRepo сохраняет сессии в Valkey.
// DI устанавливает Client при запуске, до начала обработки HTTP-запросов.
type SessionsValkeyRepo struct {
	Client valkey.Client // Подключённый клиент; временем жизни управляет DI.
}

var _ SessionsRepo = (*SessionsValkeyRepo)(nil)

// Save сохраняет имя пользователя под ключом с хешем токена.
// ctx отменяет запрос; token и username должны быть непустыми;
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
	// Этап 1: исключаем запись некорректной сессии.
	if SessionsValkeyRepo.Client == nil {
		return fmt.Errorf("SessionsRepo: Valkey client is not initialized")
	}
	if token == "" || username == "" ||
		ttl <= 0 || ttl%time.Second != 0 {
		return fmt.Errorf("SessionsRepo: invalid session data or TTL")
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
		return fmt.Errorf("SessionsRepo: save session: %w", err)
	}

	return nil
}
