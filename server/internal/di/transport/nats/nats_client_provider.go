package di_transport_nats

import (
	"context"
	"errors"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/fx"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
)

// NatsClient управляет фоновым клиентом NATS в жизненном цикле fx.
// Создаётся через NatsClientProvider; run владеет соединением и закрывает его
// при остановке. Наличие указателя natsClient не гарантирует доступность NATS:
// клиент может находиться в процессе переподключения.
// После запуска BootstrapSettings нельзя менять конкурентно с run.
// Экземпляр не следует копировать после начала использования mutex.
// Порядок запуска и остановки задаётся lifecycle fx.
//
// Схема:
//
//	fx.OnStart -> [run в goroutine] -> [клиент NATS]
//	fx.OnStop  -> cancel -> [завершение run] -> закрытие done
type NatsClient struct {
	BootstrapSettings internal.BootstrapSettings // Копия настроек; run использует NatsUrl, NatsUser и NatsPassword.
	mu                sync.Mutex                 // Защищает пару natsClient и changed.
	natsClient        *nats.Conn                 // Текущий клиент; nil между экземплярами соединения.
	changed           chan struct{}              // Закрывается при следующей замене указателя.
	cancel            context.CancelFunc         // Отмена run; устанавливается в OnStart, до запуска равна nil.
	done              chan struct{}              // Сигнал выхода из run; создаётся в OnStart и закрывается только run.
}

// NatsClientProvider создаёт владельца клиента NATS и регистрирует хуки fx.
// Сам вызов провайдера не подключается к NATS. OnStart запускает run в фоне
// и возвращает nil, не дожидаясь соединения и не передавая ошибки подключения fx.
//
// Параметры:
//   - lc — lifecycle fx, в который добавляются OnStart и OnStop; должен быть задан.
//   - BootstrapSettings — настройки подключения, копируемые в NatsClient;
//     nil недопустим и приведёт к panic при разыменовании.
//
// Результаты:
//   - *NatsClient — экземпляр с зарегистрированными хуками, ещё без соединения.
//
// Побочные эффекты:
// OnStart создаёт отдельный context от context.Background: контекст запуска fx
// не управляет временем жизни run. OnStop отменяет этот context и ждёт done.
// Если запуск не состоялся, OnStop возвращает nil; если его ctx завершился раньше
// ожидания, возвращает ctx.Err(), не гарантируя к этому моменту завершение run.
//
// Этапы:
//  1. Скопировать настройки в новый экземпляр.
//  2. Зарегистрировать запуск и остановку фоновой goroutine.
//
// Схема:
//
//	[Копия настроек] -> [Регистрация хуков] -> NatsClient
//	OnStart -> [context, cancel, done] -> [go run] -> nil
//	OnStop -> [cancel задан?]
//	             +-- нет -> nil
//	             +-- да -> cancel -> [ожидание]
//	                                    +-- done -> nil
//	                                    +-- ctx.Done -> ctx.Err()
func NatsClientProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) *NatsClient {
	// Этап 1: сохраняем собственную копию значений настроек для фонового клиента.
	NatsClient := &NatsClient{
		BootstrapSettings: *BootstrapSettings,
		changed:           make(chan struct{}),
	}

	// Этап 2: передаём управление временем жизни клиента lifecycle fx.
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			NatsClient.cancel = cancel
			NatsClient.done = make(chan struct{})
			go NatsClient.Run(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if NatsClient.cancel == nil {
				return nil
			}
			NatsClient.cancel()
			select {
			case <-NatsClient.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	return NatsClient
}

// Run создаёт клиент NATS, наблюдает за его закрытием и повторяет создание.
// Вызывается один раз из OnStart после инициализации NatsClient.done; при любом выходе
// закрывает NatsClient.done, позволяя OnStop завершить ожидание.
//
// Параметры:
//   - ctx — контекст времени жизни клиента; должен быть ненулевым. Отмена
//     прерывает ожидание закрытия и паузу между попытками. В nats.Connect
//     контекст не передаётся, поэтому текущий вызов Connect напрямую не отменяется.
//
// Побочные эффекты:
// Создаёт сетевой клиент, обновляет NatsClient.natsClient и пишет события в log.
// Ошибки создания, включая некорректный URL, логируются и повторяются до отмены.
// NATS выполняет внутренние переподключения без ограничения числа попыток;
// внешний цикл создаёт новый клиент только после ошибки Connect или ClosedHandler.
// При отмене созданное соединение закрывается через Close, без Drain.
//
// Этапы:
//  1. Настроить подключение, внутренние повторы NATS и обработчики событий.
//  2. Обработать ошибку либо сохранить клиент и ждать отмены или его закрытия.
//  3. Перед новым созданием выдержать отменяемую паузу в две секунды.
//
// Схема:
//
//	[ctx активен?] -- нет -> [выход, закрытие done]
//	     |
//	     v да
//	[nats.Connect] -> [ошибка?]
//	     +-- да -> [проверка отмены, лог ошибки] -> [пауза 2 с]
//	     +-- нет -> [проверка отмены, setConnection(conn)] -> [ожидание]
//	                                                  +-- closed -> setConnection(nil) -> [пауза 2 с]
//	                                                  +-- отмена -> Close, setConnection(nil) -> выход
//	[пауза 2 с] -> [следующая попытка либо выход при отмене]
func (NatsClient *NatsClient) Run(ctx context.Context) {
	defer close(NatsClient.done)
	const retryDelay = 2 * time.Second

	for ctx.Err() == nil {
		// Этап 1: отдельный сигнал closed отличает окончательное закрытие
		// клиента от временного разрыва, который восстанавливает сама библиотека.
		closed := make(chan struct{})
		conn, err := nats.Connect(
			NatsClient.BootstrapSettings.NatsUrl,
			nats.UserInfo(NatsClient.BootstrapSettings.NatsUser, NatsClient.BootstrapSettings.NatsPassword),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(retryDelay),
			nats.ReconnectJitter(500*time.Millisecond, time.Second),
			nats.PingInterval(10*time.Second),
			nats.MaxPingsOutstanding(2),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				if ctx.Err() == nil && err != nil {
					log.Printf("NATS: соединение потеряно: %v", err)
				}
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				if ctx.Err() == nil {
					log.Println("NATS: соединение установлено")
				}
			}),
			nats.ClosedHandler(func(conn *nats.Conn) {
				if ctx.Err() == nil {
					log.Printf("NATS: соединение закрыто, будет создано заново: %v", conn.LastError())
				}
				close(closed)
			}),
		)
		// Этап 2: ошибка создания не останавливает приложение. После успеха
		// проверяем отмену до сохранения клиента, чтобы закрыть ненужный ресурс.
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				log.Println("NATS: некорректный URL подключения, проверьте настройки")
			} else {
				log.Printf("NATS: не удалось создать клиент, повторим попытку: %v", err)
			}
		} else {
			if ctx.Err() != nil {
				conn.Close()
				return
			}
			NatsClient.setConnection(conn)
			log.Printf("NATS: клиент запущен, состояние: %s", conn.Status())
			select {
			case <-ctx.Done():
				conn.Close()
				NatsClient.setConnection(nil)
				return
			case <-closed:
				NatsClient.setConnection(nil)
			}
		}

		// Этап 3: ограничиваем частоту пересоздания клиента; отмена не ждёт таймера.
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// connectionSnapshot возвращает клиент и сигнал его замены под одной блокировкой.
// nil-клиент означает ожидание следующего подключения. Reconnect того же клиента
// сохраняет канал, поэтому работающая подписка восстанавливается силами nats.go.
func (c *NatsClient) connectionSnapshot() (*nats.Conn, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.natsClient, c.changed
}

// setConnection публикует conn и будит всех читателей прежнего снимка.
// Run передаёт nil при окончательном закрытии клиента.
func (c *NatsClient) setConnection(conn *nats.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.natsClient != conn {
		c.natsClient = conn
		close(c.changed)
		c.changed = make(chan struct{})
	}
}

// WaitConnection ждёт подключённый клиент, не создавая нового соединения.
// ctx отменяет ожидание; результат — текущий *nats.Conn либо ctx.Err().
// Соединение может разорваться сразу после возврата: вызывающий код должен
// обрабатывать ошибки последующих операций.
//
// Схема:
//
//	[ctx активен?] -> [Клиент подключён?] -- да -> Conn
//	       |                 |
//	      нет               нет -> [Пауза 100 мс] -> повтор
//	       v
//	    ctx.Err()
func (NatsClient *NatsClient) WaitConnection(ctx context.Context) (*nats.Conn, error) {
	Ticker := time.NewTicker(100 * time.Millisecond)
	defer Ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		Conn, changed := NatsClient.connectionSnapshot()
		if Conn != nil && Conn.IsConnected() {
			return Conn, nil
		}
		// Ожидание очередной проверки прерывается отменой контекста.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		case <-Ticker.C:
		}
	}
}
