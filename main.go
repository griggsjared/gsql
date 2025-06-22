package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	_ "github.com/goforj/godump"
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
		file, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		databaseFile := DatabaseFile{file}

		header, err := databaseFile.GetHeader()
		if err != nil {
			log.Fatal(err)
		}

		masterPage, err := databaseFile.GetPage(1, header.PageSize())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("database page size: %v\n", header.PageSize())
		fmt.Printf("number of tables: %d\n", masterPage.NumberCells())
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

const (
	DatabaseHeaderSize = 100
	DatabasePageSize   = 4096 // Default page size for SQLite
)

type BTreePageType int

const (
	PageTypeUnknown BTreePageType = iota
	PageTypeIndexInterior
	PageTypeTableInterior
	PageTypeIndexLeaf
	PageTypeTableLeaf
)

func (b BTreePageType) String() string {
	switch b {
	case PageTypeIndexInterior:
		return "Index Interior"
	case PageTypeTableInterior:
		return "Table Interior"
	case PageTypeIndexLeaf:
		return "Index Leaf"
	case PageTypeTableLeaf:
		return "Table Leaf"
	default:
		return "Unknown Page Type"
	}
}

type DatabaseFile struct {
	*os.File
}

func (f DatabaseFile) GetPage(number uint16, pageSize uint16) (DatabasePage, error) {
	if number == 0 {
		return nil, fmt.Errorf("page number cannot be zero")
	}

	//master page
	var offset int64
	if number == 1 {
		offset = int64(DatabaseHeaderSize)
		pageSize = uint16(pageSize) - uint16(offset)
	} else {
		offset = int64((number - 1) * pageSize)
	}

	_, err := f.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to page %d: %w", number, err)
	}

	page := NewDatabasePage(pageSize)
	used, err := f.Read(page)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", number, err)
	}

	if used < int(pageSize) {
		return nil, fmt.Errorf("read less bytes than expected for page %d: expected %d, got %d", number, pageSize, used)
	}

	return page, nil
}

func (f DatabaseFile) GetHeader() (DatabaseHeader, error) {

	_, err := f.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to the beginning of the file: %w", err)
	}

	header := NewDatabaseHeader()
	_, err = f.Read(header)
	if err != nil {
		return nil, fmt.Errorf("failed to read database header: %w", err)
	}

	return header, nil
}

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

func (p DatabasePage) PageType() BTreePageType {
	switch p[0] {
	case 0x02:
		return PageTypeIndexInterior
	case 0x05:
		return PageTypeTableInterior
	case 0x0A:
		return PageTypeIndexLeaf
	case 0x0D:
		return PageTypeTableLeaf
	default:
		return PageTypeUnknown
	}
}

func (p DatabasePage) NumberCells() uint16 {
	if len(p) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(p[3:5])
}
