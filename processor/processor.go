package processor

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"payments-rinha/model"
	"time"

	"github.com/mailru/easyjson"
)

type Processor struct {
	DefaultUrl  string
	FallbackUrl string
	Client      *http.Client
}

func NewProcessor(defaultUrl, fallbackUrl string) *Processor {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
		Timeout: 10 * time.Second,
	}
	return &Processor{DefaultUrl: defaultUrl, FallbackUrl: fallbackUrl, Client: client}
}

// SendPayment tries default first, then fallback
func (p *Processor) SendPayment(event model.PaymentEvent) (int, error) {
	if p.PostJSON(p.DefaultUrl, event) {
		return 0, nil
	}
	if p.PostJSON(p.FallbackUrl, event) {
		return 1, nil
	}
	return -1, ErrBothFailed
}

var ErrBothFailed = &ProcessorError{"Both endpoints failed"}

type ProcessorError struct {
	Message string
}

func (e *ProcessorError) Error() string {
	return e.Message
}

// Internal POST logic
func (p *Processor) PostJSON(url string, event model.PaymentEvent) bool {
	body, err := easyjson.Marshal(event)
	if err != nil {
		log.Printf("JSON marshal error: %v", err)
		return false
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/payments", url), bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Request creation error: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.Client.Do(req)
	if err != nil {
		log.Printf("HTTP error to %s: %v", url, err)
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (p *Processor) DefaultHealth() (bool, int) {
	return p.ServiceHealth(p.DefaultUrl)
}

func (p *Processor) FallbackHealth() (bool, int) {
	return p.ServiceHealth(p.FallbackUrl)
}

func (p *Processor) ServiceHealth(url string) (bool, int) {
	resp, err := p.Client.Get(fmt.Sprintf("%s/payments/service-health/", url))
	if err != nil {
		log.Printf("Service health error: %v", err)
		return true, 0
	}
	defer resp.Body.Close()

	var data model.ProcessorHealthResponse
	err = easyjson.UnmarshalFromReader(resp.Body, &data)
	if err != nil {
		return true, 0
	}
	log.Printf("Checking service health %s: %v", url, data)
	return data.Failing, data.MinResponseTime
}
