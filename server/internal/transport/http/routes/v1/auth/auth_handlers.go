package http_auth_routes_v1

import (
	"net/http"

	"github.com/gin-gonic/gin"
	http_contracts_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/contracts/v1/auth"
	auth_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
)

type HTTPAuthHandlersV1 struct {
	Login gin.HandlerFunc
}

func HTTPAuthHandlersRegistry(
	LoginUC *auth_usecases.LoginUC,
) HTTPAuthHandlersV1 {
	return HTTPAuthHandlersV1{
		// ? возможно надо будет вынести в отдельные функции
		Login: func(ctx *gin.Context) {
			var Request http_contracts_v1.LoginRequestV1
			err := ctx.ShouldBindJSON(&Request)
			if err != nil {
				ctx.JSON(http.StatusBadRequest, gin.H{"message": "invalid request body"})
				return
			}

			err = LoginUC.Execute(
				ctx.Request.Context(),
				Request.Username,
				Request.Password,
			)
			if err != nil {
				ctx.JSON(http.StatusInternalServerError, gin.H{"message": "internal server error"})
				return
			}
			ctx.JSON(http.StatusOK, http_contracts_v1.LoginResponseV1{Message: "ok"})

		},
	}
}
