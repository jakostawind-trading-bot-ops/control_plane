package di_observability

import (
	"log/slog"
	"os"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/observability/logging"
	"go.uber.org/fx"
)

// LoggerProvider создаёт общий slog-логгер на базе OpenTelemetry Logs.
// lc — обязательный lifecycle Fx; BootstrapSettings — ненулевые настройки.
// Возвращает логгер либо ошибку настройки уровня или создания exporter.
// SDK создаётся сразу, поэтому логи доступны при сборке зависимостей.
// Хук Shutdown регистрируется до хуков потребителей логгера: при обычной
// остановке Fx сначала завершает потребителей, затем выгружает остаток логов.
// Shutdown использует stop-контекст Fx; при таймауте доставка не гарантируется.
//
// Схема:
//
//	[Настройки] -> [slog + Logs SDK] -> [регистрация Shutdown] -> *slog.Logger
func LoggerProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) (*slog.Logger, error) {
	// Создаём SDK до регистрации lifecycle-хука: ошибка возвращается Fx.
	LoggerFactory := &observability.Logger{
		BootstrapSettings: *BootstrapSettings,
	}
	Logger, Provider, err := LoggerFactory.CreateOTelLogger(os.Stdout)
	if err != nil {
		return nil, err
	}

	// Shutdown дожидается пакетного экспорта в пределах stop-контекста Fx.
	lc.Append(fx.Hook{OnStop: Provider.Shutdown})
	return Logger, nil
}
