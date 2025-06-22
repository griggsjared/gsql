package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/goforj/godump"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// NOTE: sqlite internals information
// https://www.sqlite.org/fileformat.html#b_tree_pages
// https://fly.io/blog/sqlite-internals-btree/

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	fmt.Fprintln(os.Stderr, "Database file path:", databaseFilePath)
	fmt.Fprintln(os.Stderr, "Command:", command)

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := NewDatabaseHeader()
		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		_, err = databaseFile.Seek(0, 0)
		if err != nil {
			log.Fatal(err)
		}

		page := NewDatabasePage(header.PageSize())
		godump.Dump(page)

		// pages := make([]DatabasePage, 0)
		// for range header.NumberPages() {
		// 	page := NewDatabasePage(header.PageSize())
		// 	_, err = databaseFile.Read(page)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	pages = append(pages, page)
		// }

		// godump.Dump(header)
		// godump.Dump(pages)

		fmt.Printf("database page size: %v\n", header.PageSize())
		fmt.Printf("number of pages: %v\n", header.NumberPages())
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

const (
	DatabaseHeaderSize = 100
	DatabasePageSize   = 4096 // Default page size for SQLite
)

type DatabaseHeader []byte

func NewDatabaseHeader() DatabaseHeader {
	return make(DatabaseHeader, DatabaseHeaderSize)
}

func (h DatabaseHeader) PageSize() uint16 {
	return binary.BigEndian.Uint16(h[16:18])
}

func (h DatabaseHeader) NumberPages() uint32 {
	return binary.BigEndian.Uint32(h[28:32])
}

type DatabasePage []byte

func NewDatabasePage(size uint16) DatabasePage {
	if size == 0 {
		size = DatabasePageSize
	}
	return make(DatabasePage, size)
}
