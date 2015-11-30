package main

import (
		"./deleter"
	   "fmt"
	   "os"
	   )

func main() {
	d, err := deleter.NewDeleter("./conf/cachedeleter.yaml")
	if err != nil {
		fmt.Println("fail to create cache deleter:", err)
		os.Exit(-1)
	}
	fmt.Println("success init")
	err = d.Run()
	if err != nil {
		fmt.Println("fail to run cache deleter:", err)
		os.Exit(-1)
	}
}
