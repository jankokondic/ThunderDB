package meta_data

import (
	"fmt"
	"time"
)

type Manifest struct {
	ManifestId     string            `json:"manifest_id"`
	CreatedAt      time.Time         `json:"created_at"`
	Files          []ManifestFile    `json:"files"`
	PartitionValue map[string]string `json:"partition_values"`
}

type ManifestFile struct {
	Path       string               `json:"path"`
	Length     int                  `json:"length"` //bits
	RowCount   int                  `json:"row_count"`
	FileStatus map[string]FileStats `json:"file_stats"` //"file_stats": { "event_time": {"min":"2025-10-29T00:00:00","max":"2025-10-29T12:00:00", "null_count":0}, "amount":{"min":0.0,"max":1000.0} }
}

func NewManifest(old *Manifest, manifestFile ManifestFile) *Manifest {
	var files []ManifestFile

	if old != nil {
		files = old.Files
	}

	return &Manifest{
		ManifestId: NextManifestID(old.ManifestId),
		CreatedAt:  time.Now(),
		// PartitionValue: old.PartitionValue,
		Files: append(files, manifestFile),
	}
}

func NewManifestFile(Path string, Length int, RowCount int, FileState map[string]FileStats) ManifestFile {
	return ManifestFile{
		Path:       Path,
		Length:     Length,
		RowCount:   RowCount,
		FileStatus: FileState,
	}
}

func NextManifestID(latest string) string {
	now := time.Now()
	dateStr := now.Format("20060102") // YYYYMMDD

	// latest = "m-20251029-0001"
	var seq int
	if latest != "" {
		fmt.Sscanf(latest, "m-%*s-%04d", &seq)
		seq++
	} else {
		seq = 1
	}

	return fmt.Sprintf("m-%s-%04d", dateStr, seq)
}

// {
//   "path": "s3://warehouse/prod/sessions/year=2025/month=10/part-0000.parquet",
//   "length": 128934,
//   "row_count": 1000,
//   "mod_time": "2025-10-29T12:34:56Z",
//   "file_stats": {
//     "amount": {"min": 0, "max": 40},
//     "user_id": {"null_count": 5}
//   }
// }
