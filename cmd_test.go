package main

import (
	"bytes"
	"testing"
)

func TestCmd(t *testing.T) {

	t.Run("add", func(t *testing.T) {
		b := &bytes.Buffer{}
		err := addCmd.writeTo(b)
		if err != nil {
			t.Fatalf("writeTo %s", err)
			return
		}
		var cmd cmd
		if err = cmd.read(b); err != nil {
			t.Fatalf("read %s", err)
			return
		}
		if cmd != addCmd {
			t.Fatalf("cmd should be addCmd")
			return
		}
	})
}
