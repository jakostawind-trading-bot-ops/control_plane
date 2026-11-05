package di

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.uber.org/fx"
)

// CreateHTTPServer создаёт настроенный HTTP-сервер.
// address задаёт адрес прослушивания, handler обрабатывает входящие запросы.
func CreateHTTPServer(Address string, Handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              Address,
		Handler:           Handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// HTTPServerProvider передаёт запуск и остановку HTTP-сервера lifecycle Fx.
//
// Параметры:
//   - lc — lifecycle Fx для регистрации хуков; должен быть задан.
//   - server — созданный сервер, ещё не запущенный; nil недопустим.
//   - shutdowner — инициирует остановку Fx при неожиданной ошибке Serve.
//   - logger — общий логгер приложения; nil недопустим.
//
// OnStart возвращает ошибку открытия порта либо запускает Serve в goroutine.
// OnStop ждёт завершения запросов в пределах своего ctx; при ошибке Shutdown
// принудительно закрывает соединения и возвращает ошибки завершения.
//
// Схема:
//
//	OnStart -> Listen -> ошибка либо [go Serve]
//	Serve -> неожиданная ошибка -> остановка Fx с кодом 1
//	OnStop -> Shutdown(ctx) -> ошибка? -> Close
func HTTPServerProvider(
	lc fx.Lifecycle,
	server *http.Server,
	shutdowner fx.Shutdowner,
	logger *slog.Logger,
) {
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			// Открываем порт синхронно, чтобы ошибка остановила запуск Fx.
			listener, err := net.Listen("tcp", server.Addr)
			if err != nil {
				return fmt.Errorf("http: не удалось открыть %s: %w", server.Addr, err)
			}

			go func() {
				if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("HTTP-сервер завершился с ошибкой", "error", err)
					if err := shutdowner.Shutdown(fx.ExitCode(1)); err != nil {
						logger.Error("Не удалось инициировать остановку Fx", "error", err)
					}
				}
			}()

			logger.Info("HTTP-сервер запущен", "address", listener.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := server.Shutdown(ctx); err != nil {
				// Не оставляем соединения открытыми после неудачного Shutdown.
				return errors.Join(err, server.Close())
			}
			logger.InfoContext(ctx, "HTTP-сервер остановлен")
			return nil
		},
	})
}
