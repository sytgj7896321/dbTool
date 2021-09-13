package myformat

import "fmt"

func Printf(inter []interface{}) {
	for _, v := range inter {
		fmt.Println(v)
	}
}
