package transport_http_routes_v1_auth_handlers

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	entities "github.com/jakostawind-trading-bot-ops/control_plane/internal/entities/users"
	http_contracts_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/contracts/v1/auth"
	usecases_auth "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
)

func LoginHandler(
	LoginUC *usecases_auth.LoginUC,
	SessionCookieSecure bool,
) gin.HandlerFunc {
	return func(ctx *gin.Context) { // ? в аргументы функции передавать также Request и Response а не обьявлять в теле функции
		// Этап 1: некорректный запрос отклоняется до вызова LoginUC.
		var Request http_contracts_v1.LoginRequestV1
		err := ctx.ShouldBindJSON(&Request)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "invalid request body"})
			return
		}

		// Этап 2: передаём учётные данные и контекст запроса в LoginUC.
		RequestUser := entities.UserEntity{
			Username: Request.Username,
			Password: Request.Password,
		}

		Token, err := LoginUC.Execute(
			ctx.Request.Context(),
			RequestUser,
		)
		if err != nil {
			if errors.Is(err, usecases_auth.INVALID_CREDENTIALS) {
				ctx.JSON(http.StatusUnauthorized, gin.H{
					"message": "invalid username or password",
				})
				return
			}
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
			return
		}

		// Этап 3: выдаём cookie только после успешного сохранения сессии.
		// Токен передаётся в HttpOnly cookie; ответ с ним не кешируется.
		ctx.Header("Cache-Control", "no-store")
		http.SetCookie(ctx.Writer, &http.Cookie{
			Name:     "session_token",
			Value:    Token,
			Path:     "/",
			MaxAge:   int(usecases_auth.SessionTTL / time.Second),
			Secure:   SessionCookieSecure,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
		ctx.JSON(http.StatusOK, http_contracts_v1.LoginResponseV1{
			Message: "ok",
		})

	}
}
