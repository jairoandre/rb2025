package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"payments-rinha/broker"
	"payments-rinha/handler"
	"payments-rinha/postgres"
	"payments-rinha/processor"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var paymentsCounter int64

func TrackPayments(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&paymentsCounter, 1)
		handler(w, r)
	}
}

func readEnv(envName string, defaultValue string) string {
	envValue, exists := os.LookupEnv(envName)
	if exists {
		return envValue
	}
	return defaultValue
}

func main() {
	var wg sync.WaitGroup
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbHost := readEnv("DB_HOST", "localhost")

	db, err := postgres.NewDB(dbHost, 5432, "root", "root", "rb2025")
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	redisUri := readEnv("BROKER_URL", "localhost:6379")

	defaultUrl := readEnv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://localhost:8001")
	fallbackUrl := readEnv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://localhost:8002")

	pr := processor.NewProcessor(defaultUrl, fallbackUrl)

	rdb := broker.NewBrokerPool(redisUri, "admin", db, pr)

	rdb.StartListener2(ctx, &wg)

	handler := &handler.AppHandler{Broker: rdb, Db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("/payments", handler.HandlePayments)
	mux.HandleFunc("/payments-summary", handler.HandleSummary)
	mux.HandleFunc("/purge-payments", handler.HandlePurge)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "payments_requests_total %d\n", atomic.LoadInt64(&paymentsCounter))
	})

	server := &http.Server{
		Addr:    ":9999",
		Handler: mux,
	}

	go func() {
		log.Println("HTTP server running on :9999")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutdown signal received")
	// Gracefully shutdown HTTP server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	db.Pool.Close()
	log.Println("DB connection closed")
	rdb.Client.Close()
	log.Println("Broker connection closed")
	wg.Wait()
	log.Println("Application shutdown complete")

}
