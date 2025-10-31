package meta_data

import "time"

type Metadata struct {
	Database         string    `json:"database"`
	Table            string    `json:"table"`
	Location         string    `json:"location"`
	Format           string    `json:"format"`
	CreatedAt        time.Time `json:"created_at"`
	PartitionColumns []string  `json:"partition_columns"`
	Owner            string    `json:"owner"`
	Version          int       `json:"version"`
}

