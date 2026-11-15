package di_transport_http

import "github.com/gin-gonic/gin"

func GinProvider() *gin.Engine {
	GinEngine := gin.New()
	GinEngine.Use(gin.Recovery())
	return GinEngine
}