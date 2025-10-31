package main

import (
	"fmt"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	f, err := os.Open("../flights-1m.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Napravi reader
	r := parquet.NewReader(f)
	defer r.Close()

	file := r.File()

	// Ispiši osnovne informacije o fajlu
	fmt.Printf("Num RowGroups: %d\n", len(file.RowGroups()))

	// Uzmi prvi row group
	rg := file.RowGroups()[0]

	fmt.Println("column type:", rg.ColumnChunks()[0].Type().Kind())
	// Čitaj page po page
	pageReader := rg.ColumnChunks()[0].Pages()

	defer pageReader.Close()

	for {
		page, err := pageReader.ReadPage()
		if err != nil {
			// if err == parquet.ErrNoMorePages {
			// 	break
			// }
			log.Fatalf("ReadPage error: %v", err)
		}

		fmt.Println("page size:", page.Size())
		fmt.Println("number of values:", page.NumValues())
		fmt.Println("number of rows:", page.NumRows())
		fmt.Println("type:", page.Type().Kind())
		fmt.Println("column index:", page.Column())

		pageData := page.Data()
		fmt.Println(pageData.ByteArray())

		// // Svaki page sadrži dekoder
		// values := page.Values()
		// for values.Next() {
		// 	v := values.Value()
		// 	fmt.Println(v.Interface()) // <- konkretna vrednost iz kolone
		// }

		// if err := values.Err(); err != nil {
		// 	log.Fatalf("Values decode error: %v", err)
		// }
	}
}
