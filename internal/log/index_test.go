package log

import (
	"os"
	"testing"
)

func TestIndexStore_GetLog(t *testing.T) {
	s := MustNewStore(os.CreateTemp("test", ""))

	index := IndexStore{s}

	index.LastIndex()

}
