package handler

import (
	"log"
	"net/http"
	"payments-rinha/broker"
	"payments-rinha/model"
	"payments-rinha/postgres"
	"time"

	"github.com/mailru/easyjson"
)

type AppHandler struct {
	Broker *broker.BrokerPool
	Db     *postgres.DB
}

func (h *AppHandler) HandlePayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.PaymentRequest
	err := easyjson.UnmarshalFromReader(r.Body, &req)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	paymentEvent := model.PaymentEvent{
		CorrelationID: req.CorrelationID,
		Amount:        req.Amount,
		RequestedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	err = h.Broker.SendEvent(paymentEvent.ToMap(), "payments")
	if err != nil {
		log.Printf("Error sending event to redis: %v", err)
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *AppHandler) HandleSummary(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	from, err1 := time.Parse(time.RFC3339, fromStr)
	if err1 != nil {
		from = time.Now().UTC().Add(-24 * time.Hour)
	}
	to, err2 := time.Parse(time.RFC3339, toStr)
	if err2 != nil {
		to = time.Now().UTC()
	}

	res, _ := h.Db.GetSummary(from, to)

	resp := model.SummaryResponse{
		Default:  res[0],
		Fallback: res[1],
	}

	w.Header().Set("Content-Type", "application/json")
	_, _, err := easyjson.MarshalToHTTPResponseWriter(&resp, w)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (h *AppHandler) HandlePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	h.Db.PurgePayments()
	w.WriteHeader(http.StatusAccepted)
}
