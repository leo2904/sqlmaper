package main

import (
	"fmt"
	"log"

	"github.com/leo2904/sqlmaper"
)

func main() {
	q, err := sqlmaper.ParseFile("queries.sql")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("all the statements in the exact same order as they are in the provided sql file")
	iter := q.NewFileOrderIterator()
	for iter.Iterate() {
		fmt.Println(iter.Statement())
	}

	fmt.Println("")
	fmt.Println("------------------")
	fmt.Println("")

	sIter, cIter := q.NewConcurrentIterators()

	fmt.Println("all the statements that should be executed sequencialy and preserving the order as they are in the sql file")
	for sIter.Iterate() {
		fmt.Println(sIter.Statement())
	}
	fmt.Println("------------------")

	fmt.Println("all the statments that could be executed concurrently without taking care of the original order")
	for cIter.Iterate() {
		fmt.Println(cIter.Statement())
	}
}
