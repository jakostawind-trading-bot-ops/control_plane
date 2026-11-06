package http_routes

import (
	"github.com/gin-gonic/gin"
	http_auth_routes_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/routes/v1/auth"
)

func RegisterHTTPRoutes(Engine *gin.Engine, Handlers HTTPHandlers) {
	APIV1 := Engine.Group("/api/v1")
	http_auth_routes_v1.RegisterAuthRoutesV1(APIV1, Handlers.AuthV1)
}
