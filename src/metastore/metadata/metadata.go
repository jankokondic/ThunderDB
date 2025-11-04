package meta_data

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"math"
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

func (p *ParquetMetadata) GenerateKey(database, table, name string) string {
	return fmt.Sprintf("metastore/tables/%s/%s/files/%s", database, table, name)
}

func decodeMinMaxBytes(b []byte, parquetType string) interface{} {
	if len(b) == 0 {
		return nil
	}

	switch parquetType {
	case "INT32":
		if len(b) < 4 {
			return nil
		}
		return int32(binary.LittleEndian.Uint32(b))
	case "INT64":
		if len(b) < 8 {
			return nil
		}
		return int64(binary.LittleEndian.Uint64(b))
	case "FLOAT":
		if len(b) < 4 {
			return nil
		}
		bits := binary.LittleEndian.Uint32(b)
		return math.Float32frombits(bits)
	case "DOUBLE":
		if len(b) < 8 {
			return nil
		}
		bits := binary.LittleEndian.Uint64(b)
		return math.Float64frombits(bits)
	case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
		// Tekstualni string (UTF-8)
		return string(b)
	default:
		// fallback ako tip nije poznat
		return base64.StdEncoding.EncodeToString(b)
	}
}

func NewParquetMetadata(path string) *ParquetMetadata {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatalf("can't open file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatalf("can't create parquet reader: %v", err)
	}
	defer pr.ReadStop()

	info, _ := os.Stat(path)
	meta := &ParquetMetadata{
		Path:      path,
		Length:    info.Size(),
		ModTime:   info.ModTime(),
		FileStats: make(map[string]FileStats),
	}

	for _, rg := range pr.Footer.RowGroups {
		meta.RowCount += rg.NumRows
		for _, col := range rg.Columns {
			name := col.MetaData.PathInSchema[0]
			st := col.MetaData.Statistics
			if st == nil {
				continue
			}

			var nulls int64
			if st.NullCount != nil {
				nulls = *st.NullCount
			}

			minVal := decodeMinMaxBytes(st.MinValue, col.MetaData.Type.String())
			maxVal := decodeMinMaxBytes(st.MaxValue, col.MetaData.Type.String())

			meta.FileStats[name] = FileStats{
				Min:       minVal,
				Max:       maxVal,
				NullCount: nulls,
			}
		}
	}

	return meta
}
