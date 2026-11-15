package transport_http_routes_v1_auth

import (
	"github.com/gin-gonic/gin"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	transport_http_routes_v1_auth_handlers "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/routes/v1/auth/handlers"
	auth_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
)

// HTTPAuthHandlersV1 содержит обработчики авторизации API v1.
type HTTPAuthHandlersV1 struct {
	Login gin.HandlerFunc // Проверяет учётные данные и устанавливает cookie сессии.
}

// HTTPAuthHandlersRegistry создаёт обработчики авторизации.
// LoginUC проверяет credentials и сохраняет сессию до возврата токена.
// BootstrapSettings задаёт флаг Secure у cookie; обе зависимости обязательны.
// Возвращает обработчик с ответами 400, 401, 500 либо 200 с HttpOnly cookie
// и подтверждением входа в JSON. Cookie устанавливается после сохранения сессии.
//
// Схема:
//
//	JSON -> LoginUC -> ошибка -> HTTP 401/500
//	               -> токен  -> Set-Cookie + HTTP 200
func HTTPAuthHandlersRegistry(
	LoginUC *auth_usecases.LoginUC,
	BootstrapSettings *internal.BootstrapSettings,
) HTTPAuthHandlersV1 {
	return HTTPAuthHandlersV1{
		Login: transport_http_routes_v1_auth_handlers.LoginHandler(LoginUC, BootstrapSettings.SessionCookieSecure),
	}
}
