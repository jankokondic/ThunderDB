package dataframe

import (
	"fmt"

	"github.com/go-gota/gota/dataframe"
)

func main() {
	// Kreiramo DataFrame iz mape nizova (kao kolone)
	df := dataframe.LoadMaps([]map[string]interface{}{
		{"Name": "Alice", "Age": 23, "Score": 90.5},
		{"Name": "Bob", "Age": 31, "Score": 77.2},
		{"Name": "Charlie", "Age": 28, "Score": 88.8},
	})

	fmt.Println(df)

	// Filtriramo redove gde je Age > 25
	filtered := df.Filter(dataframe.F{
		Colname:    "Age",
		Comparator: ">",
		Comparando: 25,
	})
	fmt.Println("\nFiltered (Age > 25):")
	fmt.Println(filtered)

	// Biramo samo kolonu "Name"
	names := df.Select("Name")
	fmt.Println("\nNames only:")
	fmt.Println(names)
}
