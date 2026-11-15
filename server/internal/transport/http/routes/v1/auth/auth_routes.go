package transport_http_routes_v1_auth

import "github.com/gin-gonic/gin"

func RegisterAuthRoutesV1(Parent *gin.RouterGroup, Handlers HTTPAuthHandlersV1) {
	Auth := Parent.Group("/auth")
	Auth.POST("/login", Handlers.Login)
}