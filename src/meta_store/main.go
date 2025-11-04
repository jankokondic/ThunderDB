package main

import (
	"encoding/json"
	"fmt"
	schema "root/meta_data"
)

func main() {
	table := schema.NewTable("database_name", "table_name", "owner_system", nil) //create table
	out, _ := json.MarshalIndent(table, "", "  ")
	fmt.Println(string(out))

	fmt.Println("==========================")

	tableSchema := schema.CreateSchemaOnExistingFile() //create schema
	out, _ = json.MarshalIndent(tableSchema, "", "  ")
	fmt.Println(string(out))

	fmt.Println("==========================")

	metadata := schema.NewParquetMetadata("./flights-1m.parquet")
	out, _ = json.MarshalIndent(metadata, "", "  ")
	fmt.Println(string(out))

	fmt.Println("==========================")

	manifest := schema.NewManifest(&schema.Manifest{}, schema.NewManifestFile(
		"/flights-1m.parquet",
		int(metadata.Length),
		int(metadata.RowCount),
		metadata.FileStats,
	))

	out, _ = json.MarshalIndent(manifest, "", "  ")
	fmt.Println(string(out))
}
