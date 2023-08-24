package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/andro02/napredni/src"
)

func main() {

	

	wal := src.NewWal()

	reader := bufio.NewReader(os.Stdin)
	var commands = [5]string{"PUT", "GET", "DELETE", "LIST", "RANGESCAN"}
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

		fmt.Println(tokens[0])

		switch tokens[0] {

		case commands[0]:
			{
				fmt.Println("PUT code")
				src.Put(wal, tokens)
			}
		case commands[1]:
			{
				fmt.Println("GET code")
			}
		case commands[2]:
			{
				fmt.Println("DELETE code")
			}
		case commands[3]:
			{
				fmt.Println("LIST code")
			}
		case commands[4]:
			{
				fmt.Println("RANGESCAN code")
			}

		}
	}
}
