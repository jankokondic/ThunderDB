package meta_data

import "time"

type Stats struct {
	TotalRows     int       `json:"total_rows"`
	TotalSize     int       `json:"total_size"`
	NumFiles      int       `json:"num_files"`
	NumPartitions int       `json:"num_partitions"`
	UpdateAt      time.Time `json:"updated_at"`
}

// year=2025/month=10
// year=2025/month=11
// year=2026/month=1

// year=2025/month=10/
// ├── part-0000.parquet
// ├── part-0001.parquet
// ├── part-0002.parquet
// ...
// ├── part-0009.parquet

// year=2025/month=11/
// ├── part-0000.parquet
// ├── part-0001.parquet

// year=2026/month=1/
// ├── part-0000.parquet
