package main

import (
    "fmt"
    "io"
    "os"

    "github.com/colinmarc/hdfs"
)
func main() {
    localPath := "localfile.txt"
    hdfsPath := "/user/janko/locasflfasdilde.txt"

    // Create HDFS client over SSH tunnel
    client, err := hdfs.New("localhost:9000")
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot create HDFS client: %v\n", err)
        os.Exit(1)
    }
    defer client.Close()

    // Open local file
    src, err := os.Open(localPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot open local file %s: %v\n", localPath, err)
        os.Exit(1)
    }
    defer src.Close()

    // Create file in HDFS
    dst, err := client.Create(hdfsPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot create HDFS file %s: %v\n", hdfsPath, err)
        os.Exit(1)
    }

    // Copy data
    n, err := io.Copy(dst, src)
    if err != nil {
        dst.Close() // close even in case of error
        fmt.Fprintf(os.Stderr, "Error writing to HDFS: %v\n", err)
        os.Exit(1)
    }

    // Make sure to close the writer to flush data
    if err := dst.Close(); err != nil {
        fmt.Fprintf(os.Stderr, "Error closing HDFS file: %v\n", err)
        os.Exit(1)
    }

    fmt.Printf("Successfully transferred %d bytes to %s\n", n, hdfsPath)
}
