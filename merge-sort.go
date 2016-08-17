package main

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"strings"
)

const (
	maxLines           = 1000
	maxLinesAggregated = 10000
)

// FileStruct wraps a single file into a buffered read-structure to be
// used as one of the inputs in merge-sort (of previously sorted files)
type FileStruct struct {
	records     ReportEntryList
	recordsRead int
	fileName    string
	csvReader   *csv.Reader
	entriesFile *os.File
	ended       bool
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
func NewFileStruct(fileName string) *FileStruct {
	fileStruct := &FileStruct{
		fileName:    fileName,
		recordsRead: 0,
		ended:       false,
	}
	return fileStruct
}

// Init initializes the fileStruct Opens the file, and populates the first block
func (file *FileStruct) Init() bool {
	var err error
	file.entriesFile, err = os.Open(file.fileName)
	if err != nil {
		log.Printf("Could not open viewership file: %s, Error: %s\n", file.fileName, err)
		file.ended = true
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

// Close closes the file
func (file *FileStruct) Close() {
	file.ended = true
	defer file.entriesFile.Close()
}

// PeekNextItemTimestamp gets the timestamp of the next item in the file
func (file *FileStruct) PeekNextItemTimestamp() (string, error) {
	if file.ended || len(file.records) == 0 {
		file.ended = true
		defer file.Close()
		return "", errors.New("EOF")
	}
	return file.records[0].ts, nil
}

var noValueEntry = ReportEntry{}

// PopNextItem returns the next itme in the file, and moves to next entry/reads into the buffer, if needed
func (file *FileStruct) PopNextItem() ReportEntry {
	if file.Ended() {
		return noValueEntry
	}
	entry := file.records[0]

	if len(file.records) > 0 {
		file.records = file.records[1:]
	}

	if len(file.records) == 0 {
		file.ReadNextBlock()
	}
	return entry
}

// ReadNextBlock reads the next N records from the file into the buffer
func (file *FileStruct) ReadNextBlock() int {
	var i int
	for i = 0; i < maxLines; i++ {

		record, err := file.csvReader.Read()

		if err == io.EOF {
			return (i - 1)
		} else if err != nil {
			file.ended = true
			return -1
		}

		/* Report Entry
		hh_id   string
		ts      string
		pg_id   string
		pg_name string
		ch_num  string
		ch_name string
		event   string
		zipcode string
		country string

		*/
		// Reverse ts and hh_id to match the struct fields, as Sort in place for original files changes that order.
		// 										        ts,        hh_id,     pg_id,    pg_name,   ch_num,    ch_name,     event,    zipcode,   country
		// 									   2016-08-03 15:54:39,04219010,1778206157,Countdown to Rio,  52,	GOLF, 		watch,	29588,			USA
		file.records = append(file.records, ReportEntry{record[1], record[0], record[2], record[3], record[4], record[5], record[6], record[7], record[8]})
	}
	if verbose {
		log.Printf("Read: %d entries from %s \n", len(file.records), file.fileName)
	}
	return i
}

// Ended returns false if no more entries in this file
func (file *FileStruct) Ended() bool {
	return file.ended
}

// ----------------------------------------------------------------------

// FilesPack aggregates a collection of source files to be merge-sorted
type FilesPack struct {
	files []*FileStruct
}

// NewFilesPack creates and initializes new pack of files
func NewFilesPack(fileNames []string) *FilesPack {
	filePack := &FilesPack{}

	for _, fileName := range fileNames {
		fileStruct := NewFileStruct(fileName)
		if fileStruct.Init() {
			filePack.files = append(filePack.files, fileStruct)
		}
	}

	return filePack
}

// NextMinItem returns the next report entry accross all files in the pack, having the min timestamp
func (filePack *FilesPack) NextMinItem() ReportEntry {
	minTimestamp := "9999-99-99 99:99:99"
	minIndex := -1

	for i, file := range filePack.files {
		if !file.Ended() {
			ts, err := file.PeekNextItemTimestamp()

			if err != nil {
				continue
			}

			if ts < minTimestamp {
				minTimestamp = ts
				minIndex = i
			}
		}
	}

	if minIndex > -1 {
		return filePack.files[minIndex].PopNextItem()
	}

	return noValueEntry
}

// ----------------------------------------------------------------------

// AggregatedReport wraps a file allowing buffered writes into the resulting file
type AggregatedReport struct {
	file     *os.File
	filename string
	buffer   ReportEntryList
}

// NewAggregatedReport creates and initializes an instance of aggregeted report file
func NewAggregatedReport(fileName string) (*AggregatedReport, error) {
	var err error

	aggregatedReport := &AggregatedReport{
		filename: fileName,
		buffer:   ReportEntryList{},
	}

	aggregatedReport.file, err = os.Create(fileName)
	if err != nil {
		return nil, err
	}

	// write the header to the file
	header := [][]string{{"ts", "hh_id", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"}}
	write(aggregatedReport.filename, header)

	return aggregatedReport, nil
}

// ProcessFiles processes files pack merge-sort and saves the aggregated report
func (aggregated *AggregatedReport) ProcessFiles(pack *FilesPack, forDate string) {

	// 0123 45 67
	// 2016 06 01
	// 2016-06-01
	date := forDate[:4] + "-" + forDate[4:6] + "-" + forDate[6:8]

	for {
		nextItem := pack.NextMinItem()

		if nextItem == noValueEntry {
			aggregated.file.Close()
			break
		}

		if strings.Contains(nextItem.ts, date) {
			aggregated.WriteEntry(nextItem)
		}
	}

	aggregated.writeBuffer()
	aggregated.Close()
}

// WriteEntry writes an entry to the buffer, if buffer has NN values, flush to the disk
func (aggregated *AggregatedReport) WriteEntry(entry ReportEntry) bool {
	aggregated.buffer = append(aggregated.buffer, entry)

	if len(aggregated.buffer) > maxLinesAggregated {
		aggregated.writeBuffer()
		aggregated.buffer = aggregated.buffer[:0]
	}
	return true
}

// write is a utility func to write [][]string to a file->fileName
func write(filename string, content [][]string) bool {
	var f *os.File
	var err error

	if f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend); err != nil {
		log.Println("error opening file to add csv:", err)
		return false
	}

	writer := csv.NewWriter(f)

	writer.WriteAll(content)

	if err = writer.Error(); err != nil {
		log.Println("error writing csv:", err)
		return false
	}

	writer.Flush()
	f.Close()
	return true
}

// Flush the buffer to the disk/file
func (aggregated *AggregatedReport) writeBuffer() bool {
	return write(aggregated.filename, aggregated.buffer.Convert(false, false))
}

// Close closes the aggregated report file
func (aggregated *AggregatedReport) Close() {
	aggregated.file.Close()
}
