package meta_data

import (
	"log"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)

type Schema struct {
	Type       string    `json:"struct"`
	Fields     []Field   `json:"fields"`
	SchemaType string    `json:"schema_type"`
	CreatedAt  time.Time `json:"created_at"`
}

type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

func CreateSchemaOnExistingFile() *Schema {
	f, err := os.Open("./flights-1m.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r := parquet.NewReader(f)
	defer r.Close()

	return &Schema{
		Type:       "struct",
		Fields:     ReadFields(r.File()),
		SchemaType: "parquet",
		CreatedAt:  time.Now(),
	}
}

func ReadFields(file parquet.FileView) []Field {
	fields := make([]Field, len(file.Schema().Fields()))
	for index, field := range file.Schema().Fields() {
		fields[index] = Field{
			Name:     field.Name(),
			Nullable: field.Required(),
			Type:     field.Type().Kind().String(),
		}
	}

	return fields
}
