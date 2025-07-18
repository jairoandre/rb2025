package model

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

type PaymentEvent struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

func (p *PaymentEvent) ToMap() map[string]any {
	return map[string]any{
		"correlationId": p.CorrelationID,
		"amount":        p.Amount,
		"requestedAt":   p.RequestedAt,
	}
}

type Summary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type SummaryResponse struct {
	Default  Summary `json:"default"`
	Fallback Summary `json:"fallback"`
}

type ProcessorHealthResponse struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type PaymentFinishedEvent struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
	Processor     int     `json:"processor"`
}

func (p *PaymentFinishedEvent) ToMap() map[string]any {
	return map[string]any{
		"correlationId": p.CorrelationID,
		"amount":        p.Amount,
		"requestedAt":   p.RequestedAt,
		"processor":     p.Processor,
	}
}
