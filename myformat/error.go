package myformat

import (
	"fmt"
	"os"
)

func Error(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s, %s\n", msg, err)
		os.Exit(1)
	}
}
