package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandleHealth(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleHealth)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusServiceUnavailable && status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v", status)
	}
}

func TestHandleFactsMethodNotAllowed(t *testing.T) {
	req, err := http.NewRequest("POST", "/api/facts", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleFacts)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("expected %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestHandleAggregateMethodNotAllowed(t *testing.T) {
	req, err := http.NewRequest("GET", "/api/facts/aggregate", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleAggregate)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("expected %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestHandleAggregateInvalidJSON(t *testing.T) {
	body := bytes.NewBufferString("invalid json")
	req, err := http.NewRequest("POST", "/api/facts/aggregate", body)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleAggregate)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("expected %d, got %d", http.StatusBadRequest, status)
	}
}

func TestHandleAggregateInvalidGroupBy(t *testing.T) {
	req := QueryRequest{
		GroupBy: []string{"invalid_column"},
	}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequest("POST", "/api/facts/aggregate", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleAggregate)
	handler.ServeHTTP(rr, httpReq)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("expected %d, got %d", http.StatusBadRequest, status)
	}
}

func TestHandleTimeseriesMissingDates(t *testing.T) {
	req, err := http.NewRequest("GET", "/api/facts/timeseries", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleTimeseries)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("expected %d, got %d", http.StatusBadRequest, status)
	}
}

func TestHandleTimeseriesMethodNotAllowed(t *testing.T) {
	req, err := http.NewRequest("POST", "/api/facts/timeseries", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleTimeseries)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("expected %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestReflectFunction(t *testing.T) {
	tests := []struct {
		name string
		typ  string
		want interface{}
	}{
		{"DateTime", "DateTime", (*time.Time)(nil)},
		{"UInt64", "UInt64", (*uint64)(nil)},
		{"Float64", "Float64", (*float64)(nil)},
		{"String", "String", (*string)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = reflect
		})
	}
}

func TestDerefFunction(t *testing.T) {
	var f64 float64 = 42.5
	var ui64 uint64 = 100
	var s string = "test"
	tm := time.Now()

	tests := []struct {
		name string
		val  interface{}
	}{
		{"float64", &f64},
		{"uint64", &ui64},
		{"string", &s},
		{"time.Time", &tm},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deref(tt.val)
			if result == nil {
				t.Error("deref returned nil")
			}
		})
	}
}

func TestFactRowMarshalling(t *testing.T) {
	row := FactRow{
		EventDate:  "2024-01-01",
		EventTime:  "2024-01-01T10:00:00Z",
		UserID:     123,
		SessionID:  "sess-456",
		EventType:  "click",
		MetricName: "page_views",
		MetricVal:  1.0,
		Dimensions: map[string]string{"region": "US"},
	}

	data, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var unmarshalled FactRow
	if err := json.Unmarshal(data, &unmarshalled); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if unmarshalled.UserID != row.UserID {
		t.Errorf("UserID mismatch: got %d, want %d", unmarshalled.UserID, row.UserID)
	}
}
