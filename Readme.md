# Raw rows
GET /api/facts?date_from=2024-01-01&date_to=2024-01-31&event_type=purchase&limit=50

# Aggregation
POST /api/facts/aggregate
{
  "date_from": "2024-01-01",
  "date_to": "2024-01-31",
  "group_by": ["event_type", "event_date"],
  "metrics": ["sum", "count", "uniq"],
  "event_types": ["purchase", "view"]
}

# Time series
GET /api/facts/timeseries?date_from=2024-01-01&date_to=2024-03-01&metric=sum&granularity=week&event_type=purchase
```

**`go.mod`:**
```
module factapi

go 1.22

require github.com/ClickHouse/clickhouse-go/v2 v2.28.0