package main

import (
	"context"
	"data-fetcher/pipeline"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	urls = []string{
		"https://httpbin.org/delay/1",
		"https://httpbin.org/delay/2",
		"https://httpbin.org/delay/3",
		"https://httpbin.org/delay/1",
		"https://httpbin.org/delay/2",
		"https://httpbin.org/delay/3",
		"https://httpbin.org/delay/1",
		"https://httpbin.org/delay/2",
		"https://httpbin.org/status/500",
		"https://httpbin.org/delay/3",
	}
)

func main() {
	// Парсим флаги командной строки
	cancelAfter := flag.Duration("cancel-after", 0, "отменить операцию через указанное время (0 - не отменять)")
	consumeOnly := flag.Int("consume-only", 0, "получить только N результатов и отменить (0 - все)")
	concurrency := flag.Int("concurrency", 3, "максимальное количество одновременных загрузок")
	showGoroutines := flag.Bool("show-goroutines", false, "показывать количество горутин")
	flag.Parse()

	// Засекаем начальное количество горутин
	baselineGoroutines := runtime.NumGoroutine()
	log.Printf("База: %d горутин", baselineGoroutines)

	// Создаем контекст с возможностью отмены
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // гарантируем освобождение ресурсов

	// Настраиваем graceful shutdown по Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("\nПолучен сигнал прерывания, останавливаем пайплайн...")
		cancel()
	}()

	// Если указан таймаут, добавляем его в контекст
	if *cancelAfter > 0 {
		log.Printf("Устанавливаем таймаут: %v", *cancelAfter)
		ctx, cancel = context.WithTimeout(ctx, *cancelAfter)
		defer cancel()
	}

	// Создаем HTTP клиент с таймаутами
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Создаем DataFetcher
	fetcher := pipeline.NewDataFetcher(client, *concurrency)

	// Запускаем мониторинг горутин в реальном времени
	if *showGoroutines {
		go monitorGoroutines(ctx, baselineGoroutines)
	}

	// Запускаем пайплайн
	log.Printf("Запускаем загрузку %d URL с конкурентностью %d", len(urls), *concurrency)
	startTime := time.Now()
	results := fetcher.Fetch(ctx, urls)

	// Счетчики для статистики
	var received, errors int32

	// Обрабатываем результаты
resultLoop:
	for i := 0; i < len(urls); i++ {
		select {
		case result, ok := <-results:
			if !ok {
				log.Println("Канал результатов закрыт")
				break resultLoop
			}

			atomic.AddInt32(&received, 1)

			if result.Err != nil {
				atomic.AddInt32(&errors, 1)
				log.Printf("❌ Ошибка [%s]: %v", result.URL, result.Err)
			} else {
				log.Printf("✅ Успех [%s]: %d байт", result.URL, len(result.Body))
			}

			// Если нужно получить только N результатов
			if *consumeOnly > 0 && int(received) >= *consumeOnly {
				log.Printf("Получено %d результатов, отменяем...", received)
				cancel()
				// Продолжаем читать оставшиеся результаты (они могут быть в буфере)
			}

		case <-ctx.Done():
			// Контекст отменен, но результаты могут еще быть в канале
			log.Println("Контекст отменен, дочитываем оставшиеся результаты...")

			// Пытаемся дочитать то, что уже в канале
			for {
				select {
				case result, ok := <-results:
					if !ok {
						break resultLoop
					}
					atomic.AddInt32(&received, 1)
					if result.Err != nil {
						atomic.AddInt32(&errors, 1)
					}
					log.Printf("📦 Дополнительный результат [%s]: %d байт (ошибка: %v)",
						result.URL, len(result.Body), result.Err)
				default:
					// В канале больше нет результатов
					break resultLoop
				}
			}
		}
	}

	// Ждем немного, чтобы горутины успели завершиться
	time.Sleep(100 * time.Millisecond)

	// Выводим статистику
	finalGoroutines := runtime.NumGoroutine()
	log.Printf("\n=== Статистика ===")
	log.Printf("Время выполнения: %v", time.Since(startTime))
	log.Printf("Получено результатов: %d", received)
	log.Printf("Ошибок: %d", errors)
	log.Printf("Горутины: %d → %d (разница: %d)",
		baselineGoroutines, finalGoroutines, finalGoroutines-baselineGoroutines)

	if finalGoroutines > baselineGoroutines+5 {
		log.Printf("⚠️  Возможная утечка горутин! Разница: %d", finalGoroutines-baselineGoroutines)
	} else {
		log.Printf("✅ Утечек горутин не обнаружено")
	}
}

// monitorGoroutines показывает динамику изменения количества горутин
func monitorGoroutines(ctx context.Context, baseline int) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var maxGoroutines int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := runtime.NumGoroutine()
			if current > maxGoroutines {
				maxGoroutines = current
			}
			log.Printf("📊 Горутины: %d (пик: %d, база: %d, diff: %d)",
				current, maxGoroutines, baseline, current-baseline)
		}
	}
}
