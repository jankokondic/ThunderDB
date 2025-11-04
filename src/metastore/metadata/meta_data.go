package meta_data

import (
	"path"
	"time"
)

const TABLE_PATH string = "metastore/tables"

type TableProperties struct {
	Database         string    `json:"database"`
	Table            string    `json:"table"`
	Location         string    `json:"location"`
	Format           string    `json:"format"`
	CreatedAt        time.Time `json:"created_at"`
	PartitionColumns []string  `json:"partition_columns"`
	Owner            string    `json:"owner"`
	Version          int       `json:"version"`
}

func NewTable(Database, Table, Owner string, PartitionColumns []string) *TableProperties {
	return &TableProperties{
		Database:         Database,
		Table:            Table,
		Location:         path.Join(TABLE_PATH, Database, Table),
		Format:           "parquet",
		PartitionColumns: PartitionColumns,
		CreatedAt:        time.Now(),
		Owner:            Owner,
		Version:          1,
	}
}
