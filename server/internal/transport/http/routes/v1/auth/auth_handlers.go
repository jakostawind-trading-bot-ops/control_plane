package http_auth_routes_v1

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
	http_contracts_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/contracts/v1/auth"
	auth_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
)

type HTTPAuthHandlersV1 struct {
	Login gin.HandlerFunc
}

// HTTPAuthHandlersRegistry создаёт обработчики авторизации.
// LoginUC проверяет credentials и сохраняет сессию до возврата токена.
// Возвращает обработчик с ответами 400, 401, 500 либо 200 с токеном в JSON.
//
// Схема:
//
//	JSON -> LoginUC -> ошибка -> HTTP 401/500
//	               -> токен  -> HTTP 200
func HTTPAuthHandlersRegistry(
	LoginUC *auth_usecases.LoginUC,
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
			// Ответ содержит секретный токен и не должен кешироваться.
			ctx.Header("Cache-Control", "no-store")
			ctx.JSON(http.StatusOK, http_contracts_v1.LoginResponseV1{
				Message:      "ok",
				SessionToken: Token,
			})

		},
	}
}
