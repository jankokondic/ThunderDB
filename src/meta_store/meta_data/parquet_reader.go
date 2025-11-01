package meta_data

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type ParquetMetadata struct {
	Path      string
	Length    int64
	ModTime   time.Time
	RowCount  int64
	FileStats map[string]FileStats `json:"file_stats"`
}

type FileStats struct {
	Min       interface{} `json:"min,omitempty"`
	Max       interface{} `json:"max,omitempty"`
	NullCount int64       `json:"null_count,omitempty"`
}

func ReadParquet(path string) *ParquetMetadata {
	// Otvori fajl
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatal("Can't open file:", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatal("Can't create parquet reader:", err)
	}
	defer pr.ReadStop()

	// Statistika fajla
	info, _ := os.Stat(path)
	fileMeta := ParquetMetadata{
		Path:      path,
		Length:    info.Size(),
		ModTime:   info.ModTime(),
		FileStats: make(map[string]FileStats),
	}

	// Broj redova
	for _, rg := range pr.Footer.RowGroups {
		fileMeta.RowCount += rg.NumRows
		for _, col := range rg.Columns {
			name := col.MetaData.PathInSchema[0]
			st := col.MetaData.Statistics
			if st != nil {
				fileMeta.FileStats[name] = FileStats{
					Min:       st.MinValue,
					Max:       st.MaxValue,
					NullCount: *st.NullCount,
				}
			}
		}
	}

	out, _ := json.MarshalIndent(fileMeta, "", "  ")
	fmt.Println(string(out))

	return &fileMeta
}
