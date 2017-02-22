package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/attic-labs/graphql"
	"github.com/attic-labs/graphql/testutil"
)

func main() {
	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()["query"][0]
		result := graphql.Do(graphql.Params{
			Schema:        testutil.StarWarsSchema,
			RequestString: query,
		})
		json.NewEncoder(w).Encode(result)
	})
	fmt.Println("Now server is running on port 8080")
	fmt.Println("Test with Get      : curl -g 'http://localhost:8080/graphql?query={hero{name}}'")
	http.ListenAndServe(":8080", nil)
}
