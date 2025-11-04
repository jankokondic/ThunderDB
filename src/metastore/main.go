package main

import (
	"encoding/json"
	"fmt"
	"log"
	"root/etcd"
	schema "root/metadata"
	"time"
)

func main() {
	cfg := etcd.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	etcd, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer etcd.Close()

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
