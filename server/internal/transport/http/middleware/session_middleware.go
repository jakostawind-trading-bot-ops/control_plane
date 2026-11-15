package transport_http_middleware

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	sessions_repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/sessions"
)

// SessionUsernameKey — ключ подтверждённого имени пользователя в gin.Context.
// Хендлер получает имя через ctx.GetString(SessionUsernameKey).
const SessionUsernameKey = "auth.session.username"

// SessionMiddleware допускает запрос к следующему хендлеру по действующей сессии.
// SessionsRepo читает сессии; Logger записывает инфраструктурные ошибки.
// Обе зависимости обязательны и должны быть инициализированы до запросов.
// Возвращает middleware, читающий cookie session_token и сохраняющий имя
// пользователя под SessionUsernameKey в gin.Context.
// Отсутствующая cookie или сессия даёт 401; ошибка хранилища — 500.
// При отказе цепочка прерывается. TTL сессии не продлевается.
//
// Этапы:
//  1. Получить токен из cookie.
//  2. Проверить сессию через репозиторий.
//  3. Передать установленного пользователя следующему хендлеру.
//
// Схема:
//
//	cookie -> SessionsRepo -> username -> gin.Context -> следующий хендлер
//	   |           +-- сессия отсутствует -> 401 + Abort
//	   |           +-- ошибка хранилища  -> 500 + Abort
//	   +-- нет токена -> 401 + Abort
func SessionMiddleware(
	SessionsRepo sessions_repositories.SessionsRepo,
	Logger *slog.Logger,
) gin.HandlerFunc {
	Logger = Logger.With("component", "session_middleware")

	return func(ctx *gin.Context) {
		// Этап 1: запрос без токена отклоняется до обращения к репозиторию.
		Token, err := ctx.Cookie("session_token")
		if err != nil || Token == "" {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"message": "unauthorized",
			})
			return
		}

		// Этап 2: сбой хранилища не подменяется отсутствием сессии.
		Username, err := SessionsRepo.GetByToken(ctx.Request.Context(), Token)
		if err != nil {
			if errors.Is(err, sessions_repositories.SESSION_NOT_FOUND) {
				ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
					"message": "unauthorized",
				})
				return
			}

			Logger.ErrorContext(ctx.Request.Context(), "Не удалось проверить сессию",
				"error", err,
			)
			ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"message": "internal server error",
			})
			return
		}

		// Этап 3: в контекст попадает имя из хранилища, токен не передаётся.
		ctx.Set(SessionUsernameKey, Username)
		ctx.Next()
	}
}
