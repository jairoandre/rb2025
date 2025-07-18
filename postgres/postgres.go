package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"payments-rinha/model"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	Pool *sql.DB
}

// Connect to PostgreSQL with a connection pool
func NewDB(host string, port int, user, password, dbname string) (*DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// Optional: tune pool settings
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	log.Println("Connected to PostgreSQL")
	return &DB{Pool: db}, nil
}

// Insert a payment record
func (db *DB) InsertPayment(correlationID string, amount float64, processor int, requested_at time.Time) error {
	query := `
        INSERT INTO payments (id, amount, processor, requested_at)
        VALUES ($1, $2, $3, $4)
    `
	_, err := db.Pool.Exec(query, correlationID, amount, processor, requested_at)
	return err
}

// Get summary grouped by processor
func (db *DB) GetSummary(from, to time.Time) (map[int]model.Summary, error) {
	query := `
        SELECT processor, COUNT(*) AS total_requests, SUM(amount) AS total_amount
        FROM payments
        WHERE requested_at BETWEEN $1 AND $2
        GROUP BY processor
    `
	rows, err := db.Pool.Query(query, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summary := map[int]model.Summary{0: {TotalRequests: 0, TotalAmount: 0}, 1: {TotalRequests: 0, TotalAmount: 0}}
	for rows.Next() {
		var processor int
		var totalRequests int
		var totalAmount float64
		if err := rows.Scan(&processor, &totalRequests, &totalAmount); err != nil {
			return nil, err
		}
		summary[processor] = model.Summary{TotalRequests: totalRequests, TotalAmount: totalAmount}
	}

	return summary, nil
}

func (db *DB) PurgePayments() {
	query := `DELETE FROM payments`
	_, err := db.Pool.Exec(query)
	if err != nil {
		log.Printf("Error on purge payments: %v", err)
	}
}
