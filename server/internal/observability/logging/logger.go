package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/processors/minsev"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
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

// CreateOTelLogger создаёт slog-логгер с экспортом OTel-записей в Out.
// Out — обязательный writer; DI передаёт os.Stdout.
// Возвращает логгер и SDK-провайдер, чей Shutdown обязан вызвать владелец.
// Ошибка означает некорректный LoggerLevel либо отказ создания exporter.
// Логгер передаёт ctx каждой записи в SDK для извлечения trace_id и span_id.
// Формат JSON задаёт stdoutlog; прежний ReplaceAttr не применяется.
//
// Этапы:
//  1. Проверить уровень и создать stdout exporter.
//  2. Описать сервис и создать SDK с фильтром уровней перед batch processor.
//  3. Подключить otelslog к созданному SDK без глобальной регистрации.
//
// Схема:
//
//	slog -> otelslog -> SDK -> minsev -> batch -> stdoutlog -> Out
func (Logger *Logger) CreateOTelLogger(Out io.Writer) (*slog.Logger, *sdklog.LoggerProvider, error) {
	// Этап 1: ошибочные настройки отклоняем до создания фонового processor.
	Level, err := Logger.GetLevel()
	if err != nil {
		return nil, nil, err
	}
	Exporter, err := stdoutlog.New(stdoutlog.WithWriter(Out))
	if err != nil {
		return nil, nil, fmt.Errorf("logging: создание stdout exporter: %w", err)
	}

	// Этап 2: resource связывает записи с сервисом и окружением приложения.
	Attributes := []attribute.KeyValue{
		attribute.String("service.name", "control-plane-server"),
	}
	if Stage := strings.TrimSpace(Logger.BootstrapSettings.Stage); Stage != "" {
		Attributes = append(Attributes, attribute.String("deployment.environment.name", Stage))
	}
	Provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(resource.NewSchemaless(Attributes...)),
		sdklog.WithProcessor(minsev.NewLogProcessor(
			sdklog.NewBatchProcessor(Exporter),
			// minsev использует шкалу slog: DEBUG = -4, INFO = 0.
			minsev.Severity(Level),
		)),
	)

	// Этап 3: прикладной код продолжает получать обычный *slog.Logger.
	return otelslog.NewLogger(
		"github.com/jakostawind-trading-bot-ops/control_plane",
		otelslog.WithLoggerProvider(Provider),
		otelslog.WithSource(true),
	), Provider, nil
}
