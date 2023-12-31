package src

import "fmt"

func Put(wal *Wal, memtable *Memtable, tokens []string) {

	if len(tokens) != 3 {
		fmt.Println("Invalid input. Please try again.")
		return
	}

	var key string = tokens[1]
	var value string = tokens[2]

	walEntry := wal.Write(key, []byte(value), 0)
	memtable.Set(key, walEntry.ToBytes())

}
