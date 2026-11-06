package http_contracts_v1

// api/v1/auth/login
type LoginRequestV1 struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponseV1 struct {
	Message string `json:"message"`
}
