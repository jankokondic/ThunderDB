package main

import (
	"encoding/json"
	"fmt"
	schema "root/meta_data"
	"time"
)

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

func main() {
	schema := schema.CreateSchemaOnExistingFile()
	out, _ := json.MarshalIndent(schema, "", "  ")
	fmt.Println(string(out))
	// f, err := os.Open("./flights-1m.parquet")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer f.Close()

	// // Napravi reader
	// r := parquet.NewReader(f)
	// defer r.Close()

	// file := r.File()

	// fmt.Println(file.Schema().Type())
	// fmt.Println(file.Schema().Name())
	// fmt.Println(file.Schema().GoType())
	// for _, field := range file.Schema().Fields() {
	// 	fmt.Printf("name: %s | type: %s | required: %v \n", field.Name(), field.Type(), field.Required())
	// }

	// // Ispiši osnovne informacije o fajlu
	// fmt.Printf("Num RowGroups: %d\n", len(file.RowGroups()))

	// // Uzmi prvi row group
	// rg := file.RowGroups()[0]

	// fmt.Println("column type:", rg.ColumnChunks()[0].Type().Kind())
	// // Čitaj page po page
	// pageReader := rg.ColumnChunks()[0].Pages()

	// defer pageReader.Close()

	// for {
	// 	page, err := pageReader.ReadPage()
	// 	if err != nil {
	// 		// if err == parquet.ErrNoMorePages {
	// 		// 	break
	// 		// }
	// 		log.Fatalf("ReadPage error: %v", err)
	// 	}

	// 	fmt.Println("page size:", page.Size())
	// 	fmt.Println("number of values:", page.NumValues())
	// 	fmt.Println("number of rows:", page.NumRows())
	// 	fmt.Println("type:", page.Type().Kind())
	// 	fmt.Println("column index:", page.Column())

	// 	pageData := page.Data()
	// 	fmt.Println(pageData.ByteArray())

	// 	// // Svaki page sadrži dekoder
	// 	// values := page.Values()
	// 	// for values.Next() {
	// 	// 	v := values.Value()
	// 	// 	fmt.Println(v.Interface()) // <- konkretna vrednost iz kolone
	// 	// }

	// 	// if err := values.Err(); err != nil {
	// 	// 	log.Fatalf("Values decode error: %v", err)
	// 	// }
	// }
}
