package test

import "fmt"

func DiffMessage(actual, expected any, desc string) string {
	return fmt.Sprintf("\n\x1b[32m%v\x1b[0m\n\x1b[2mExpected:\x1b[0m \x1b[31m%v\x1b[0m\n\x1b[2mActual:\x1b[0m \x1b[31m%v\x1b[0m", desc, expected, actual)
}
