package src

import "fmt"

func Delete(wal *Wal, memtable *Memtable, tokens []string) {

	if len(tokens) != 3 {
		fmt.Println("Invalid input. Please try again.")
		return
	}

	var key string = tokens[1]
	var value string = tokens[2]

	walEntry := wal.Write(key, []byte(value), 1)
	walEntry.Tombstone = 1
	memtable.Delete(key, walEntry.ToBytes())

}
