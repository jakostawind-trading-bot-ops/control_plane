package http_contracts_v1

// api/v1/auth/login
type LoginRequestV1 struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponseV1 подтверждает вход; токен передаётся через HttpOnly cookie.
type LoginResponseV1 struct {
	Message string `json:"message"` // Статус успешного входа.
}
