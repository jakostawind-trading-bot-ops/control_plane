package repositories

import (
	"context"
	"time"
)

// SessionsRepo хранит пользовательские сессии с ограниченным сроком жизни.
// Используется LoginUC; реализация отвечает за сохранение и истечение сессии.
type SessionsRepo interface {
	// Save сохраняет новую сессию без перезаписи существующей.
	// ctx отменяет обращение к хранилищу; token — непустой секретный токен;
	// username — непустое имя пользователя; ttl — положительный срок
	// жизни в целых секундах. Возвращает ошибку проверки или сохранения.
	Save(ctx context.Context, token, username string, ttl time.Duration) error
}
