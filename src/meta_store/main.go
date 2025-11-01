package main

import (
	"encoding/json"
	"fmt"
	schema "root/meta_data"
)

func main() {

	metadata := schema.NewMetadata("database_name", "table_name", "owner_system", nil)
	out, _ := json.MarshalIndent(metadata, "", "  ")
	fmt.Println(string(out))

	tableSchema := schema.CreateSchemaOnExistingFile() //helper function
	out, _ = json.MarshalIndent(tableSchema, "", "  ")
	fmt.Println(string(out))

	newFileSchema := schema.ReadParquet("./flights-1m.parquet")
	_ = newFileSchema
}
