package src

import "fmt"

func Put(wal *Wal, bTree *BTree, tokens []string) {

	if len(tokens) != 3 {
		fmt.Println("Invalid input. Please try again.")
	}

	var key string = tokens[1]
	var value string = tokens[2]

	wal.Write(key, []byte(value))
	//bTree.Insert(key, []byte(value))
	fmt.Printf("")

}
