package main

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
)

const (
	maxLines = 1000
)

// FileStruct wraps a single file into a buffered read-structure to be
// used as one of the inputs in merge-sort (of previously sorted files)
type FileStruct struct {
	records     ReportEntryList
	recordsRead int
	fileName    string
	csvReader   *csv.Reader
	entriesFile *os.File
}

// NewFileStruct initializes and returns new wrapper instance for fileName
//
// Pattern to use:
// fileStruct := NewFileStruct("sourceFileName.csv")
// if fileStruct.Init() {
//		ts, err := fileStruct.PeeknextItemTimestamp()
//
//		if err != nill { return "End of file"}
//		. . .
// 		item := fileStruct.PopNextItem()
//		. . .
//		fileStruct.Close()
//
// }
func NewFileStruct(fileName string) FileStruct {
	fileStruct := FileStruct{
		fileName:    fileName,
		recordsRead: 0,
	}
	return fileStruct
}

// Init initializes the fileStruct Opens the file, and populates the first block
func (file *FileStruct) Init() bool {
	var err error
	file.entriesFile, err = os.Open(file.fileName)
	if err != nil {
		log.Printf("Could not open viewership file: %s, Error: %s\n", file.fileName, err)
		return false
	}

	file.csvReader = csv.NewReader(file.entriesFile)

	// Skipping the first line - header
	// "hh_id", "ts", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"
	_, err = file.csvReader.Read()
	if err != nil {
		return false
	}
	file.ReadNextBlock()
	return true
}

func (file *FileStruct) Close() {
	defer file.entriesFile.Close()
}

// PeekNextItemTimestamp gets the timestamp of the next item in the file
func (file *FileStruct) PeekNextItemTimestamp() (string, error) {
	if len(file.records) == 0 {
		return "", errors.New("EOF")
	}
	return file.records[0].ts, nil
}

// PopNextItem returns the next itme in the file, and moves to next entry/reads into the buffer, if needed
func (file *FileStruct) PopNextItem() ReportEntry {
	entry := file.records[0]

	file.records = file.records[1:]

	if len(file.records) == 0 {
		file.ReadNextBlock()
	}
	return entry
}

// ReadNextBlock reads the next N records from the file into the buffer
func (file *FileStruct) ReadNextBlock() int {
	// TODO ReadALl - to convert to read block of NN lines each time
	var i int
	for i = 0; i < maxLines; i++ {

		record, err := file.csvReader.Read()

		if err == io.EOF {
			return (i - 1)
		} else if err != nil {
			return -1
		}

		// 	---			---			0		1		2		3			4		5			6		7			8
		// 						 "hh_id", "ts", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"
		file.records = append(file.records, ReportEntry{record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]})
	}
	if verbose {
		log.Printf("Read: %d entries from %s \n", len(file.records), file.fileName)
	}
	return i
}

// ----------------------------------------------------------------------

// FilesPack aggregates a collection of source files to be merge-sorted
type FilesPack struct {
	files []FileStruct
}
