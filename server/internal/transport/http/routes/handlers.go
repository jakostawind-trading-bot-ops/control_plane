package http_routes

import http_auth_routes_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/routes/v1/auth"

type HTTPHandlers struct {
	AuthV1 http_auth_routes_v1.HTTPAuthHandlersV1
}

func HTTPhandlersRegistry(
	HTTPAuthHandlersV1 http_auth_routes_v1.HTTPAuthHandlersV1,
) HTTPHandlers {
	return HTTPHandlers{
		AuthV1: HTTPAuthHandlersV1,
	}
}