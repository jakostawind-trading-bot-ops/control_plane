package di

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	tracing "github.com/jakostawind-trading-bot-ops/control_plane/internal/observability/tracing"
	"go.uber.org/fx"
)

// TracerProvider создаёт общий tracing и связывает его запуск и остановку с Fx.
//
// Параметры:
//   - lc — lifecycle Fx для регистрации Start и Shutdown; обязателен.
//   - BootstrapSettings — ненулевые настройки приложения; Stage получен из STAGE.
//
// Результат — объект tracing, который будет запущен в OnStart.
// Конструктор только задаёт настройки и регистрирует хуки. Ошибки Start
// и Shutdown возвращаются Fx через соответствующие lifecycle-хуки.
// В main объект востребуется отдельным Invoke до создания подписчика:
// это обеспечивает запуск tracing раньше подписчика и остановку после него.
//
// Схема:
//
//	[Настройки] -> [Tracing + регистрация хуков] -> *Tracing
//	OnStart -> Tracing.Start
//	OnStop  -> Tracing.Shutdown
func TracerProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) *tracing.Tracing {
	Tracing := &tracing.Tracing{
		ServiceName: "control-plane-server",
		Stage:       BootstrapSettings.Stage,
	}

	lc.Append(fx.Hook{
		OnStart: Tracing.Start,
		OnStop:  Tracing.Shutdown,
	})

	return Tracing
}
