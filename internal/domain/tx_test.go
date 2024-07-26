package domain

import (
	"encoding/json"
	"fmt"
	fuzz "github.com/google/gofuzz"
	"testing"
)

func Test(t *testing.T) {
	tx := Transaction{}

	f := fuzz.New()
	f.Fuzz(&tx)
	d, _ := json.MarshalIndent(tx, "", "		")
	fmt.Println("", string(d))
}
