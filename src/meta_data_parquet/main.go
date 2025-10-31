package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type ColumnMeta struct {
	PathInSchema          []string `json:"path_in_schema"`
	Type                  string   `json:"type"`
	Codec                 string   `json:"codec"`
	NumValues             int64    `json:"num_values"`
	TotalCompressedSize   int64    `json:"total_compressed_size"`
	TotalUncompressedSize int64    `json:"total_uncompressed_size"`
	DataPageOffset        int64    `json:"data_page_offset"`
	MinValue              string   `json:"min_value,omitempty"`
	MaxValue              string   `json:"max_value,omitempty"`
	NullCount             *int64   `json:"null_count,omitempty"`
}

type RowGroupMeta struct {
	NumRows       int64        `json:"num_rows"`
	TotalByteSize int64        `json:"total_byte_size"`
	Columns       []ColumnMeta `json:"columns"`
}

type FileMetadata struct {
	NumRows      int64             `json:"num_rows"`
	CreatedBy    string            `json:"created_by,omitempty"`
	Schema       []string          `json:"schema"`
	Version      int32             `json:"version"`
	KeyValueMeta map[string]string `json:"key_value_meta,omitempty"`
	RowGroups    []RowGroupMeta    `json:"row_groups"`
}

func main() {
	// Opening parquet file
	fr, err := local.NewLocalFileReader("../flights-1m.parquet")
	if err != nil {
		log.Fatalf("cannot open file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatalf("cannot create parquet reader: %v", err)
	}
	defer pr.ReadStop()

	if err := pr.ReadFooter(); err != nil {
		log.Fatalf("cannot read footer: %v", err)
	}

	footer := pr.Footer

	meta := FileMetadata{
		NumRows:   footer.GetNumRows(),
		CreatedBy: footer.GetCreatedBy(),
		Version:   footer.GetVersion(),
		Schema:    []string{},
	}

	// Schema
	for _, se := range footer.GetSchema() {
		meta.Schema = append(meta.Schema, se.GetName())
	}

	// Key-Value meta data
	if kvs := footer.GetKeyValueMetadata(); kvs != nil {
		meta.KeyValueMeta = map[string]string{}
		for _, kv := range kvs {
			if kv != nil {
				meta.KeyValueMeta[kv.GetKey()] = kv.GetValue()
			}
		}
	}

	// RowGroup and columns
	for _, rg := range footer.GetRowGroups() {
		rgMeta := RowGroupMeta{
			NumRows:       rg.GetNumRows(),
			TotalByteSize: rg.GetTotalByteSize(),
		}

		for _, col := range rg.GetColumns() {
			md := col.GetMetaData()
			if md == nil {
				continue
			}

			colMeta := ColumnMeta{
				PathInSchema:          md.GetPathInSchema(),
				Type:                  md.GetType().String(),
				Codec:                 md.GetCodec().String(),
				NumValues:             md.GetNumValues(),
				TotalCompressedSize:   md.GetTotalCompressedSize(),
				TotalUncompressedSize: md.GetTotalUncompressedSize(),
				DataPageOffset:        md.GetDataPageOffset(),
			}

			if stats := md.GetStatistics(); stats != nil {
				if stats.IsSetMinValue() {
					colMeta.MinValue = string(stats.GetMinValue())
				}
				if stats.IsSetMaxValue() {
					colMeta.MaxValue = string(stats.GetMaxValue())
				}
				if stats.IsSetNullCount() {
					n := stats.GetNullCount()
					colMeta.NullCount = &n
				}
			}

			rgMeta.Columns = append(rgMeta.Columns, colMeta)
		}

		meta.RowGroups = append(meta.RowGroups, rgMeta)
	}

	out, _ := json.MarshalIndent(meta, "", "  ")
	fmt.Println(string(out))

	total := pr.GetNumRows()
	batchSize := 1000

	for read := int64(0); read < total; {
		n := batchSize
		if total-read < int64(batchSize) {
			n = int(total - read)
		}

		rows := make([]interface{}, n)
		if err := pr.Read(&rows); err != nil {
			log.Fatalf("read error: %v", err)
		}

		for _, row := range rows {
			// row je tipa map[string]interface{}
			r := row.(map[string]interface{})
			fmt.Printf("FL_DATE=%v, DEP_DELAY=%v, ARR_DELAY=%v\n",
				r["FL_DATE"], r["DEP_DELAY"], r["ARR_DELAY"])
		}

		read += int64(n)
	}

}
