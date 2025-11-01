package main

import (
    "fmt"
    "io"
    "os"

    "github.com/colinmarc/hdfs"
)
func main() {
    hdfsPath := "/user/janko/locasflfasdilde.txt"

    // Create HDFS client
    client, err := hdfs.New("localhost:9000")
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot create HDFS client: %v\n", err)
        os.Exit(1)
    }
    defer client.Close()

    // Open HDFS file for reading
    file, err := client.Open(hdfsPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot open HDFS file %s: %v\n", hdfsPath, err)
        os.Exit(1)
    }
    defer file.Close()

    // Jump first 10 bytes
    offset := int64(2)
    _, err = file.Seek(offset, io.SeekStart)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot seek to position %d: %v\n", offset, err)
        os.Exit(1)
    }

    content, err := io.ReadAll(file)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error reading HDFS file content: %v\n", err)
        os.Exit(1)
    }

    fmt.Printf("File content from byte %d:\n%s\n", offset, string(content))
}
