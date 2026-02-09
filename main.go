package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Config
const (
	chAddr     = "localhost:9000"
	chDB       = "default"
	chUser     = "default"
	chPassword = ""
	listenAddr = ":8080"
)

// FactRow represents a row from the fact table
type FactRow struct {
	EventDate  string            `json:"event_date"`
	EventTime  string            `json:"event_time"`
	UserID     uint64            `json:"user_id"`
	SessionID  string            `json:"session_id"`
	EventType  string            `json:"event_type"`
	MetricName string            `json:"metric_name"`
	MetricVal  float64           `json:"metric_value"`
	Dimensions map[string]string `json:"dimensions"`
}

// QueryRequest represents a query from the client
type QueryRequest struct {
	DateFrom   string            `json:"date_from"`   // 2024-01-01
	DateTo     string            `json:"date_to"`     // 2024-01-31
	EventTypes []string          `json:"event_types"` // filter by event type
	UserIDs    []uint64          `json:"user_ids"`    // filter by user
	GroupBy    []string          `json:"group_by"`    // group by columns
	Metrics    []string          `json:"metrics"`     // sum, count, avg on metric_value
	Filters    map[string]string `json:"filters"`     // dimension filters
	Limit      int               `json:"limit"`
	Offset     int               `json:"offset"`
}

// AggResult for grouped queries
type AggResult struct {
	Groups map[string]string  `json:"groups"`
	Values map[string]float64 `json:"values"`
}

type APIResponse struct {
	Data  any    `json:"data"`
	Count int    `json:"count"`
	Error string `json:"error,omitempty"`
}

var conn driver.Conn

func main() {
	var err error
	conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{
			Database: chDB,
			Username: chUser,
			Password: chPassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal("cannot ping clickhouse:", err)
	}
	log.Println("Connected to ClickHouse")

	http.HandleFunc("/api/facts", handleFacts)
	http.HandleFunc("/api/facts/aggregate", handleAggregate)
	http.HandleFunc("/api/facts/timeseries", handleTimeseries)
	http.HandleFunc("/health", handleHealth)

	log.Printf("Listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

// GET /api/facts?date_from=...&date_to=...&event_type=...&user_id=...&limit=100&offset=0
func handleFacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	q := r.URL.Query()
	dateFrom := q.Get("date_from")
	dateTo := q.Get("date_to")
	eventType := q.Get("event_type")
	userID := q.Get("user_id")
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))

	if limit <= 0 || limit > 10000 {
		limit = 100
	}

	// Build query with parameterized conditions
	var conditions []string
	args := make([]any, 0)
	argIdx := 0

	if dateFrom != "" {
		conditions = append(conditions, "event_date >= ?")
		args = append(args, dateFrom)
		argIdx++
	}
	if dateTo != "" {
		conditions = append(conditions, "event_date <= ?")
		args = append(args, dateTo)
		argIdx++
	}
	if eventType != "" {
		conditions = append(conditions, "event_type = ?")
		args = append(args, eventType)
		argIdx++
	}
	if userID != "" {
		uid, err := strconv.ParseUint(userID, 10, 64)
		if err == nil {
			conditions = append(conditions, "user_id = ?")
			args = append(args, uid)
			argIdx++
		}
	}

	query := "SELECT event_date, event_time, user_id, session_id, event_type, metric_name, metric_value FROM facts"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY event_time DESC LIMIT %d OFFSET %d", limit, offset)

	_ = argIdx
	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}
	defer rows.Close()

	var results []FactRow
	for rows.Next() {
		var row FactRow
		var eventDate time.Time
		var eventTime time.Time
		if err := rows.Scan(&eventDate, &eventTime, &row.UserID, &row.SessionID,
			&row.EventType, &row.MetricName, &row.MetricVal); err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
			return
		}
		row.EventDate = eventDate.Format("2006-01-02")
		row.EventTime = eventTime.Format(time.RFC3339)
		results = append(results, row)
	}

	writeJSON(w, http.StatusOK, APIResponse{Data: results, Count: len(results)})
}

// POST /api/facts/aggregate
func handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid request body"})
		return
	}

	// Validate group by columns (whitelist to prevent injection)
	allowedCols := map[string]bool{
		"event_date": true, "event_type": true, "metric_name": true,
		"user_id": true, "session_id": true,
	}
	for _, col := range req.GroupBy {
		if !allowedCols[col] {
			writeJSON(w, http.StatusBadRequest, APIResponse{Error: fmt.Sprintf("invalid group_by column: %s", col)})
			return
		}
	}

	// Validate metrics
	allowedMetrics := map[string]string{
		"sum":   "sum(metric_value)",
		"avg":   "avg(metric_value)",
		"count": "count()",
		"min":   "min(metric_value)",
		"max":   "max(metric_value)",
		"uniq":  "uniq(user_id)",
	}
	if len(req.Metrics) == 0 {
		req.Metrics = []string{"sum", "count"}
	}

	// Build SELECT
	var selectParts []string
	for _, col := range req.GroupBy {
		selectParts = append(selectParts, col)
	}
	for _, m := range req.Metrics {
		expr, ok := allowedMetrics[m]
		if !ok {
			writeJSON(w, http.StatusBadRequest, APIResponse{Error: fmt.Sprintf("invalid metric: %s", m)})
			return
		}
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", expr, m))
	}

	// Build WHERE
	var conditions []string
	var args []any

	if req.DateFrom != "" {
		conditions = append(conditions, "event_date >= ?")
		args = append(args, req.DateFrom)
	}
	if req.DateTo != "" {
		conditions = append(conditions, "event_date <= ?")
		args = append(args, req.DateTo)
	}
	if len(req.EventTypes) > 0 {
		placeholders := make([]string, len(req.EventTypes))
		for i, et := range req.EventTypes {
			placeholders[i] = "?"
			args = append(args, et)
		}
		conditions = append(conditions, "event_type IN ("+strings.Join(placeholders, ",")+")")
	}
	if len(req.UserIDs) > 0 {
		placeholders := make([]string, len(req.UserIDs))
		for i, uid := range req.UserIDs {
			placeholders[i] = "?"
			args = append(args, uid)
		}
		conditions = append(conditions, "user_id IN ("+strings.Join(placeholders, ",")+")")
	}

	query := "SELECT " + strings.Join(selectParts, ", ") + " FROM facts"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	if len(req.GroupBy) > 0 {
		query += " GROUP BY " + strings.Join(req.GroupBy, ", ")
	}
	query += " ORDER BY " + req.Metrics[0] + " DESC"

	limit := req.Limit
	if limit <= 0 || limit > 10000 {
		limit = 100
	}
	query += fmt.Sprintf(" LIMIT %d", limit)
	if req.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", req.Offset)
	}

	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}
	defer rows.Close()

	colTypes := rows.ColumnTypes()
	colNames := rows.Columns()

	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(colNames))
		for i, ct := range colTypes {
			vals[i] = reflect(ct)
		}
		if err := rows.Scan(vals...); err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
			return
		}
		row := make(map[string]any)
		for i, name := range colNames {
			row[name] = deref(vals[i])
		}
		results = append(results, row)
	}

	writeJSON(w, http.StatusOK, APIResponse{Data: results, Count: len(results)})
}

// GET /api/facts/timeseries?date_from=...&date_to=...&event_type=...&metric=sum&granularity=day
func handleTimeseries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Error: "method not allowed"})
		return
	}

	q := r.URL.Query()
	dateFrom := q.Get("date_from")
	dateTo := q.Get("date_to")
	eventType := q.Get("event_type")
	metric := q.Get("metric")
	granularity := q.Get("granularity")

	if dateFrom == "" || dateTo == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Error: "date_from and date_to required"})
		return
	}

	metricExpr := "sum(metric_value)"
	switch metric {
	case "avg":
		metricExpr = "avg(metric_value)"
	case "count":
		metricExpr = "count()"
	case "uniq":
		metricExpr = "uniq(user_id)"
	}

	dateExpr := "event_date"
	switch granularity {
	case "hour":
		dateExpr = "toStartOfHour(event_time)"
	case "week":
		dateExpr = "toMonday(event_date)"
	case "month":
		dateExpr = "toStartOfMonth(event_date)"
	}

	var conditions []string
	var args []any

	conditions = append(conditions, "event_date >= ?", "event_date <= ?")
	args = append(args, dateFrom, dateTo)

	if eventType != "" {
		conditions = append(conditions, "event_type = ?")
		args = append(args, eventType)
	}

	query := fmt.Sprintf(
		"SELECT %s AS period, %s AS value FROM facts WHERE %s GROUP BY period ORDER BY period",
		dateExpr, metricExpr, strings.Join(conditions, " AND "),
	)

	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}
	defer rows.Close()

	type Point struct {
		Period string  `json:"period"`
		Value  float64 `json:"value"`
	}
	var results []Point
	for rows.Next() {
		var p Point
		var t time.Time
		if err := rows.Scan(&t, &p.Value); err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
			return
		}
		p.Period = t.Format("2006-01-02T15:04:05")
		results = append(results, p)
	}

	writeJSON(w, http.StatusOK, APIResponse{Data: results, Count: len(results)})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	err := conn.Ping(context.Background())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// Helper to allocate scan targets based on column type
func reflect(ct driver.ColumnType) any {
	switch ct.DatabaseTypeName() {
	case "Date", "DateTime", "DateTime64":
		return new(time.Time)
	case "UInt64":
		return new(uint64)
	case "Float64":
		return new(float64)
	case "String", "LowCardinality(String)", "FixedString":
		return new(string)
	default:
		return new(any)
	}
}

func deref(v any) any {
	switch p := v.(type) {
	case *time.Time:
		return p.Format("2006-01-02T15:04:05")
	case *uint64:
		return *p
	case *float64:
		return *p
	case *string:
		return *p
	case *any:
		return *p
	default:
		return v
	}
}
