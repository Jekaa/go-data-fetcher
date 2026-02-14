package pipeline

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type Result struct {
	URL  string
	Body []byte
	Err  error
}

// DataFetcher реализует контекстно-зависимый пайплайн загрузки данных.
type DataFetcher struct {
	client        *http.Client
	maxConcurrent int
}

func NewDataFetcher(client *http.Client, maxConcurrent int) *DataFetcher {
	if client == nil {
		client = http.DefaultClient
	}
	return &DataFetcher{
		client:        client,
		maxConcurrent: maxConcurrent,
	}
}

// Fetch запускает конкурентную загрузку URL'ов и возвращает канал с результатами.
// Гарантии:
//   - Канал будет закрыт после завершения всех загрузок или при отмене контекста.
//   - При отмене контекста все горутины завершаются без блокировок.
//   - Результаты, полученные до отмены, будут доставлены.
func (f *DataFetcher) Fetch(ctx context.Context, urls []string) <-chan Result {
	// Выходной канал создается здесь - это единственное место, где он будет закрыт.
	// Это следует принципу "кто создает канал, тот его и закрывает" (channel ownership).
	out := make(chan Result)

	// Запускаем единственную горутину-координатора, которая будет управлять загрузками.
	// Это гарантирует, что канал будет закрыт ровно один раз.
	go f.run(ctx, urls, out)

	return out
}

// Внутренний координатор пайплайна.
func (f *DataFetcher) run(ctx context.Context, urls []string, out chan<- Result) {
	defer close(out)

	done := make(chan struct{})

	concurrency := len(urls)
	if f.maxConcurrent > 0 && f.maxConcurrent < concurrency {
		concurrency = f.maxConcurrent
	}

	semaphore := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	wg.Add(len(urls))

	for _, url := range urls {
		// Захватываем url для замыкания
		url := url

		go func() {
			defer wg.Done()

			// Пытаемся захватить семафор с учетом отмены контекста
			select {
			case semaphore <- struct{}{}:
				// Получили разрешение, продолжаем
			case <-ctx.Done():
				// Контекст отменен до того, как мы получили разрешение
				return
			}

			// Освобождаем семафор после завершения загрузки
			defer func() { <-semaphore }()

			// Выполняем загрузку с контекстом
			result := f.fetchURL(ctx, url)

			// Отправляем результат с учетом отмены контекста.
			// Это критически важное место: используем select для проверки отмены
			// перед отправкой, чтобы избежать блокировки на отправке в закрытый канал
			// или при отмене контекста.
			select {
			case out <- result:
				// Результат успешно отправлен
			case <-ctx.Done():
				// Контекст отменен - не пытаемся отправить результат,
				// так как получатель уже мог закрыть соединение
				return
			}
		}()
	}

	// Запускаем горутину для ожидания завершения всех загрузок
	go func() {
		wg.Wait()
		close(done)
	}()

	// Ожидаем либо завершения всех загрузок, либо отмены контекста
	select {
	case <-done:
		// Все загрузки завершены нормально
		return
	case <-ctx.Done():
		// Контекст отменен - позволим горутинам завершиться через defer
		// и выйдем из run(), закрыв out
		return
	}
}

// fetchURL выполняет HTTP запрос и возвращает Result.
// Контекст используется для отмены запроса на транспортном уровне.
func (f *DataFetcher) fetchURL(ctx context.Context, url string) Result {
	// Создаем запрос с контекстом - это позволит HTTP клиенту корректно обработать
	// отмену на уровне TCP соединения
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, Err: fmt.Errorf("create request: %w", err)}
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return Result{URL: url, Err: fmt.Errorf("do request: %w", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{URL: url, Err: fmt.Errorf("read body: %w", err)}
	}

	if resp.StatusCode != http.StatusOK {
		return Result{
			URL:  url,
			Body: body,
			Err:  fmt.Errorf("unexpected status: %s", resp.Status),
		}
	}

	return Result{URL: url, Body: body}
}
