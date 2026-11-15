package repositories_sessions

import (
	"context"
	"errors"
	"time"
)

// SESSION_NOT_FOUND означает отсутствие действующей сессии для токена.
var SESSION_NOT_FOUND = errors.New("SessionsRepo: session not found")

// SessionsRepo хранит и читает пользовательские сессии с ограниченным сроком жизни.
// Используется LoginUC и middleware; реализация контролирует истечение сессии.
type SessionsRepo interface {
	// Save сохраняет новую сессию без перезаписи существующей.
	// ctx отменяет обращение к хранилищу; token — непустой секретный токен;
	// username — непустое имя пользователя; ttl — положительный срок
	// жизни в целых секундах. Возвращает ошибку проверки или сохранения.
	Save(ctx context.Context, token, username string, ttl time.Duration) error

	// GetByToken возвращает непустое имя владельца действующей сессии.
	// ctx отменяет обращение к хранилищу; token — секретный токен сессии.
	// Пустой token, отсутствующая или истёкшая сессия дают SESSION_NOT_FOUND.
	// Ошибки хранилища возвращаются отдельно; при любой ошибке имя пустое.
	// Чтение не продлевает срок жизни сессии.
	GetByToken(ctx context.Context, token string) (string, error)
}
