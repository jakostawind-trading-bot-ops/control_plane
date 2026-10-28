package observability

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
)

type Logger struct {
	BootstrapSettings internal.BootstrapSettings
}

func (Logger *Logger) GetLevel() (slog.Level, error) {
	switch Logger.BootstrapSettings.LoggerLevel {
	case "DEBUG":
		return slog.LevelDebug, nil
	case "INFO":
		return slog.LevelInfo, nil
	default:
		return slog.LevelInfo, fmt.Errorf(
			"неизвестный уровень логирования: %q",
			Logger.BootstrapSettings.LoggerLevel,
		)
	}
}

// Create создаёт JSON-логгер, записывающий события в Out.
// Возвращает ошибку при некорректном уровне логирования.
func (Logger *Logger) CreateJSONLogger(Out io.Writer) (*slog.Logger, error) {
	// Получаем минимальный уровень из настроек.
	Level, err := Logger.GetLevel()
	if err != nil {
		return nil, err
	}

	return slog.New(slog.NewJSONHandler(
		Out,
		&slog.HandlerOptions{
			Level:     Level,
			AddSource: true, // Включаем сведения о месте вызова логгера.

			ReplaceAttr: func(Groups []string, Attr slog.Attr) slog.Attr {
				// Вложенные пользовательские поля оставляем без изменений.
				if len(Groups) > 0 {
					return Attr
				}

				switch Attr.Key {
				case slog.TimeKey:
					if Attr.Value.Kind() == slog.KindTime {
						// Переименовываем time и форматируем время в UTC.
						return slog.String(
							"timestamp",
							Attr.Value.Time().UTC().Format(time.RFC3339Nano),
						)
					}

				case slog.SourceKey:
					Source, ok := Attr.Value.Any().(*slog.Source)
					if !ok || Source == nil {
						return Attr
					}

					// Пустое имя группы помещает поля прямо в корень JSON.
					return slog.Group("",
						slog.String("file", Source.File),
						slog.String("func", Source.Function),
						slog.Int("line", Source.Line),
					)
				}

				return Attr
			},
		},
	)), nil
}
