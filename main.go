package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/andro02/napredni/config"
	"github.com/andro02/napredni/src"
)

func main() {
	cfg, err := config.ReadConfig("config.txt")
	if err != nil {
		panic(err)
	}
	config.LoadValues(cfg)

	tb := src.CreateTokenBucket(float64(config.TOKEN_BUCKET_MAX_TOKENS), float64(config.TOKEN_BUCKET_REFILL))
	wal := src.NewWal()
	memtable := src.NewMT()
	cache := src.Init()

	//test(wal, memtable, cache)
	//return
	// path := "sstable//1693756325_"
	// file, _ := os.Open(path + "data.bin")
	// for {
	// 	fmt.Println(src.ReadWalEntry(file))
	// }
	// src.TestIndex(path)
	// src.TestSummary(path)
	//return

	reader := bufio.NewReader(os.Stdin)
	var commands = [7]string{"PUT", "GET", "DELETE", "LIST", "RANGESCAN", "CMS", "HLL"}
	var tokens []string

	for {
		fmt.Print(">>> ")
		input, err := reader.ReadString('\n')
		input = strings.TrimRight(input, "\r\n")

		if err != nil {
			log.Fatal(err)
		}
		tokens = strings.Split(input, " ")

		var found bool = false
		for _, command := range commands {
			if tokens[0] == command {
				found = true
				break
			}
		}

		if !found {
			fmt.Println("Invalid command. Please try again.")
			continue
		}

		if !tb.Request(5) {
			fmt.Println("Too many requests.")
			continue
		}

		switch tokens[0] {

		case commands[0]:
			{
				src.Put(wal, memtable, tokens)
			}
		case commands[1]:
			{
				value, tombstone := src.Get(memtable, cache, tokens)
				fmt.Println(value, tombstone)
			}
		case commands[2]:
			{
				src.Delete(wal, memtable, tokens)
			}
		case commands[3]:
			{
				fmt.Println("LIST code")
			}
		case commands[4]:
			{
				fmt.Println("RANGESCAN code")
			}
		case commands[5]:
			{
				fmt.Println("Count-Min-Sketch Code")
				src.CMSMenu()
			}
		case commands[6]:
			{
				fmt.Println("Hyper-Log-Log Code")
				src.HLLMenu()
			}

		}
	}
}

func test(wal *src.Wal, memtable *src.Memtable, cache *src.LRUCache) {

	size := 1000

	rand.New(rand.NewSource(time.Now().UnixNano()))

	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")

	keys := make([]string, 0)
	values := make([]string, 0)
	tombstones := make([]byte, 0)

	for i := 0; i < size; i++ {

		tokens := make([]string, 3)
		key := make([]rune, rand.Intn(20)+1)
		for i := range key {
			key[i] = letters[rand.Intn(len(letters))]
		}

		value := make([]rune, rand.Intn(20)+1)
		for i := range value {
			value[i] = letters[rand.Intn(len(letters))]
		}

		tokens[0] = "PUT"
		tokens[1] = string(key)
		tokens[2] = string(value)

		// get := make([]string, 2)
		// get[0] = "GET"
		// get[1] = string(key)

		keys = append(keys, string(key))
		values = append(values, string(value))
		tombstones = append(tombstones, 0)

		//fmt.Println(tokens[1], " ", tokens[2])
		src.Put(wal, memtable, tokens)
		// value1, tombstone := src.Get(memtable, cache, get)
		// if value1 != string(value) {
		// 	fmt.Println("Error: ", get[1], " ", string(value), tombstone, " ", string(value), 0)
		// } else {
		// 	fmt.Println("Success: ", get[1], " ", value, tombstone, " ", string(value), 0)
		// }

	}

	for i := 0; i < size; i++ {

		tokens := make([]string, 2)

		tokens[0] = "GET"
		tokens[1] = keys[i]

		value, tombstone := src.Get(memtable, cache, tokens)
		if value != values[i] {
			fmt.Println("Error: ", keys[i], " ", value, tombstone, " ", values[i], tombstones[i])
		} else {
			fmt.Println("Success: ", keys[i], " ", value, tombstone, " ", values[i], tombstones[i])
		}
	}

}
