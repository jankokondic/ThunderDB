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

	if err := etcd.DeleteAll("", 5*time.Second); err != nil {
		log.Fatal(err)
	}

	// fmt.Println("==========================")

	var (
		databaseName string = "database_name"
		tableName    string = "table_name"
	)

	table := schema.NewTable(databaseName, tableName, "owner_system", nil) //create table
	out, _ := json.MarshalIndent(table, "", "  ")
	// fmt.Println(string(out))

	if err := etcd.Put(table.GenerateKey(), string(out), 10*time.Second); err != nil {
		log.Println(err)
		return
	}

	// fmt.Println("==========================")

	tableSchema := schema.CreateSchemaOnExistingFile() //create schema
	out, _ = json.MarshalIndent(tableSchema, "", "  ")
	// fmt.Println(string(out))

	if err := etcd.Put(tableSchema.GenerateKey(databaseName, tableName), string(out), 10*time.Second); err != nil {
		log.Println(err)
		return
	}

	// fmt.Println("==========================")

	metadata := schema.NewParquetMetadata("./flights-1m.parquet")
	out, _ = json.MarshalIndent(metadata, "", "  ")
	// fmt.Println(string(out))

	if err := etcd.Put(metadata.GenerateKey(databaseName, tableName, "flights-1m.parquet"), string(out), 10*time.Second); err != nil {
		log.Println(err)
		return
	}

	// fmt.Println("==========================")

	manifest := schema.NewManifest(&schema.Manifest{}, schema.NewManifestFile(
		"/flights-1m.parquet",
		int(metadata.Length),
		int(metadata.RowCount),
		metadata.FileStats,
	))

	out, _ = json.MarshalIndent(manifest, "", "  ")
	// fmt.Println(string(out))

	if err := etcd.Put(manifest.GenerateKey(databaseName, tableName), string(out), 10*time.Second); err != nil {
		log.Println(err)
		return
	}

	//update manifest last table
	if err := etcd.Put(GenerateLastManifestKey(databaseName, tableName), manifest.ManifestId, 10*time.Second); err != nil {
		log.Println(err)
		return
	}

	allKeys, err := etcd.GetAll("", 5*time.Second) // "" for all keys
	if err != nil {
		log.Fatal(err)
	}

	for k, v := range allKeys {
		fmt.Printf("%s = %s\n", k, v)
	}
}

func GenerateLastManifestKey(database, table string) string {
	return fmt.Sprintf("metastore/tables/%s/%s/latest_manifest", database, table)
}
