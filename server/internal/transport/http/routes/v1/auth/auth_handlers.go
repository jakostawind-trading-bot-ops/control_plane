package http_auth_routes_v1

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
	http_contracts_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/contracts/v1/auth"
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
		// ? возможно надо будет вынести в отдельные функции
		Login: func(ctx *gin.Context) { // ? в аргументы функции передавать также Request и Response а не обьявлять в теле функции
			var Request http_contracts_v1.LoginRequestV1
			err := ctx.ShouldBindJSON(&Request)
			if err != nil {
				ctx.JSON(http.StatusBadRequest, gin.H{"message": "invalid request body"})
				return
			}

			RequestUser := entities.UserEntity{
				Username: Request.Username,
				Password: Request.Password,
			}

			Token, err := LoginUC.Execute(
				ctx.Request.Context(),
				RequestUser,
			)
			if err != nil {
				if errors.Is(err, auth_usecases.INVALID_CREDENTIALS) {
					ctx.JSON(http.StatusUnauthorized, gin.H{
						"message": "invalid username or password",
					})
					return
				}
				ctx.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
				return
			}
			// Токен передаётся в HttpOnly cookie; ответ с ним не кешируется.
			ctx.Header("Cache-Control", "no-store")
			http.SetCookie(ctx.Writer, &http.Cookie{
				Name:     "session_token",
				Value:    Token,
				Path:     "/",
				MaxAge:   int(auth_usecases.SessionTTL / time.Second),
				Secure:   BootstrapSettings.SessionCookieSecure,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})
			ctx.JSON(http.StatusOK, http_contracts_v1.LoginResponseV1{
				Message: "ok",
			})

		},
	}
}
