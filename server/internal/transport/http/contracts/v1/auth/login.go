package http_contracts_v1

// api/v1/auth/login
type LoginRequestV1 struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponseV1 содержит результат входа и секретный токен сессии.
type LoginResponseV1 struct {
	Message      string `json:"message"`       // Статус успешного входа.
	SessionToken string `json:"session_token"` // Токен доступа; не предназначен для логов.
}
