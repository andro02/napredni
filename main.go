package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {

	reader := bufio.NewReader(os.Stdin)
	var commands = [5]string{"PUT", "GET", "DELETE", "LIST", "RANGESCAN"}
	var tokens []string

	for {
		fmt.Print(">>> ")
		input, err := reader.ReadString('\n')
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

		if found {
			fmt.Println(tokens[0])
			break
		} else {
			fmt.Println("Invalid command. Please try again.")
		}
	}

	switch tokens[0] {

	case commands[0]:
		{
			fmt.Println("PUT code")
			src.put()
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
