package http_auth_routes_v1

import "github.com/gin-gonic/gin"

func RegisterAuthRoutesV1(Parent *gin.RouterGroup, Handlers HTTPAuthHandlersV1) {
	Auth := Parent.Group("/auth")
	Auth.POST("/login", Handlers.Login)
}