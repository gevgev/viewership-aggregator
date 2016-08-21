package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func formatDefaultDate() string {
	year, month, day := time.Now().Date()

	return fmt.Sprintf("%4d%02d%02d", year, int(month), day)
}

func formatDate(date string) string {
	return strings.Replace(strings.Replace(date, "/", "", -1), "-", "", -1)
}

const (
	version = "0.1"
	// MAXATTEMPTS max attempts to download file from AWS S3
	MAXATTEMPTS = 3
)

var (
	regionName      string
	bucketName      string
	dateFrom        string
	dateTo          string
	msoListFilename string
	maxAttempts     int
	concurrency     int
	daysAfter       int

	verbose bool
	testRun bool
	appName string

	failedFilesChan         chan string
	downloadedReportChannel chan bool

	// MSOLookup is map of MSO IDs to MSO names
	MSOLookup map[string]string
	msoList   []MsoType
)

func init() {

	flagRegion := flag.String("r", "us-west-2", "`AWS Region`")
	flagBucket := flag.String("b", "daap-viewership-reports", "`Bucket name`")
	flagDateFrom := flag.String("from", formatDefaultDate(), "`Date from`")
	flagDateTo := flag.String("to", formatDefaultDate(), "`Date to`")
	flagMsoFileName := flag.String("m", "mso-list.csv", "Filename for `MSO` list")
	flagMaxAttempts := flag.Int("M", MAXATTEMPTS, "`Max attempts` to retry download from aws.s3")
	flagConcurrency := flag.Int("c", 10, "The number of files to process `concurrent`ly")
	flagDaysAfter := flag.Int("d", 2, "The number of days to go back for the report")
	flagHelp := flag.Bool("h", false, "Help")
	flagTestRun := flag.Bool("t", false, "Test run to dump full csv as well")

	flagVerbose := flag.Bool("v", true, "`Verbose`: outputs to the screen")

	flag.Parse()
	if flag.Parsed() {
		appName = os.Args[0]

		if *flagHelp {
			usage()
		}

		regionName = *flagRegion
		bucketName = *flagBucket
		dateFrom = formatDate(*flagDateFrom)
		dateTo = formatDate(*flagDateTo)
		msoListFilename = *flagMsoFileName
		maxAttempts = *flagMaxAttempts
		concurrency = *flagConcurrency
		daysAfter = *flagDaysAfter

		verbose = *flagVerbose
		testRun = *flagTestRun

		if verbose {
			fmt.Printf("Provided From: %s, converted to %s\n", *flagDateFrom, dateFrom)
			fmt.Printf("Provided To: %s, converted to %s\n", *flagDateTo, dateTo)
		}
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -r <aws_region> -b <s3_bucket_name> --from <date> --to <date> -d <days to aggregate> -m <mso-list-file-name> -M <max_retry>\n", appName)
	flag.Usage()
	os.Exit(-1)
}

// PrintParams prints out the parameters provided to the app
func PrintParams() {
	log.Printf("Provided: -r: %s, -b: %s, -from: %v, -to: %v, -d %d -m %s, -M %d, -v: %v\n",
		regionName,
		bucketName,
		dateFrom,
		dateTo,
		daysAfter,
		msoListFilename,
		maxAttempts,
		verbose,
	)

}

// MsoType aggregates MSO code and name
type MsoType struct {
	Code string
	Name string
}

// getMsoNamesList reads the list of MSO's and initialize the lookup map and array
func getMsoNamesList() ([]MsoType, map[string]string) {
	msoList := []MsoType{}
	msoLookup := make(map[string]string)

	msoFile, err := os.Open(msoListFilename)
	if err != nil {
		log.Fatalf("Could not open Mso List file: %s, Error: %s\n", msoListFilename, err)
	}

	r := csv.NewReader(msoFile)
	r.TrimLeadingSpace = true

	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Could not read MSO file: %s, Error: %s\n", msoListFilename, err)
	}

	for _, record := range records {
		msoList = append(msoList, MsoType{record[0], record[1]})
		msoLookup[record[0]] = record[1]
	}
	return msoList, msoLookup
}

// formatPrefix formats path per mso
func formatPrefix(path, msoCode string) string {
	return fmt.Sprintf("%s/%s/delta/", path, msoCode)
}

// convertToDateParts converts/breaks the "20160601" string into yy, mm, dd
func convertToDateParts(dtStr string) (yy, mm, dd int) {
	// 0123 45 67
	// 2016 06 01
	//
	yy, mm, dd = 0, 0, 0
	i, err := strconv.Atoi(dtStr[:4])
	if err != nil {
		return yy, mm, dd
	}
	yy = i

	i, err = strconv.Atoi(dtStr[4:6])
	if err != nil {
		return yy, mm, dd
	}
	mm = i

	i, err = strconv.Atoi(dtStr[6:])
	if err != nil {
		return yy, mm, dd
	}
	dd = i
	return yy, mm, dd
}

// getDateRange generates
// a list of strings for each date in range to lookup
// starting one day before (from - 1) -to- N daysAfter (to + daysAfter)
func getDateRange(dateFrom, dateTo string, daysAfter int) []string {

	regExpStr := []string{}
	//'20160630'
	yy, mm, dd := convertToDateParts(dateFrom)
	dtFrom := time.Date(yy, time.Month(mm), dd, 0, 0, 0, 0, time.UTC)
	if verbose {
		log.Printf("Provided From: string:%s, parsed into: %d, %d, %d, converted into %v\n",
			dateFrom, yy, mm, dd, dtFrom.String())
	}

	yy, mm, dd = convertToDateParts(dateTo)
	dtTo := time.Date(yy, time.Month(mm), dd, 0, 0, 0, 0, time.UTC)

	if verbose {
		log.Printf("Provided To: string:%s, parsed into: %d, %d, %d, converted into %v\n",
			dateTo, yy, mm, dd, dtTo.String())
	}

	if dtFrom.After(dtTo) { // || dtFrom.Equal(dtTo) {
		log.Printf("Date from %v is greater or equal than date to: %v\n", dtFrom, dtTo)
		log.Println("Nothing to do")
		os.Exit(-1)
	}

	dtFrom = dtFrom.AddDate(0, 0, -1)
	dtTo = dtTo.AddDate(0, 0, daysAfter)

	if verbose {
		log.Println("Working From:", dtFrom.String())
		log.Println("Working To:", dtTo.String())
	}

	dt := dtFrom
	for {
		regExpStr = append(regExpStr, dt.Format("20060102"))
		if verbose {
			log.Printf("Appending for %s = %s\n", dt.String(), dt.Format("20060102"))
		}

		dt = dt.AddDate(0, 0, 1)
		if dt.After(dtTo) {
			break
		}
	}

	return regExpStr
}

func printRangeString(dateRangeRegexStr []string) {
	log.Println("Dates range:")
	for _, str := range dateRangeRegexStr {
		log.Println(str)
	}
}

func main() {
	startTime := time.Now()
	countingDone := make(chan bool)

	// This is our semaphore/pool
	sem := make(chan bool, concurrency)

	downloaded := 0

	failedFilesChan = make(chan string)
	downloadedReportChannel = make(chan bool)

	msoList, MSOLookup = getMsoNamesList()

	if verbose {
		PrintParams()
	}

	dateRange := getDateRange(dateFrom, dateTo, daysAfter)

	failedFilesList := []string{}
	var wg sync.WaitGroup

	// Listening to failed reports
	go func() {
		for {
			key, more := <-failedFilesChan
			if more {
				failedFilesList = append(failedFilesList, key)
			} else {
				return
			}
		}
	}()

	// listening to succeeded reports
	go func() {
		for {
			_, more := <-downloadedReportChannel
			if more {
				downloaded++
			} else {
				countingDone <- true
				return
			}
		}
	}()

	session := session.New(&aws.Config{
		Region: aws.String(regionName),
	})

	svc := s3.New(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName), // daap-viewership-reports
		Prefix: aws.String("cdw-viewership-reports"),
	}

	// Get the list of all objects
	resp, err := svc.ListObjects(params)
	if err != nil {
		log.Println("Failed to list objects: ", err)
		os.Exit(-1)
	}

	log.Println("Number of objects: ", len(resp.Contents))
	for _, key := range resp.Contents {
		// iterate through the list to match the dates range/mso name
		// using the constracted below lookup string

		if verbose {
			log.Println("Key: ", *key.Key)
		}

		for _, mso := range msoList {

			for _, eachDate := range dateRange {
				// cdw-data-reports/20160601/ Armstrong-Butler/hhid_count- Armstrong-Butler-20160601.csv
				lookupKey := fmt.Sprintf("%s-%s.csv", mso.Name, eachDate)

				if verbose {
					log.Println("Lookup key: ", lookupKey)
				}

				if strings.Contains(*key.Key, lookupKey) {
					// download the file (add to a queue of downloads)
					// load the csv file, add the count to appropriate counter
					// if we still have available goroutine in the pool (out of concurrency )
					sem <- true
					wg.Add(1)
					go func(key string) {
						defer func() { <-sem }()
						processSingleDownload(key, &wg)
					}(*key.Key)
				}
			}

		}
	}

	// Now aggregate the counts and generate the aggregated report
	// save the report csv?

	// Reports
	if verbose {
		log.Println("All files sent to be downloaded. Waiting for completetion...")
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	wg.Wait()
	if verbose {
		log.Println("All download jobs completed, closing failed/succeeded jobs channel")
	}

	close(failedFilesChan)
	close(downloadedReportChannel)
	<-countingDone
	close(countingDone)

	ReportFailedFiles(failedFilesList)

	GenerateDailyAggregatesMergeSort(dateFrom, dateRange, daysAfter)

	log.Printf("Processed %d MSO's, %d days, in %v\n", len(msoList), len(dateRange), time.Since(startTime))
}

// GenerateDailyAggregatesMergeSort generates the aggregated reports using merge-sort from files
func GenerateDailyAggregatesMergeSort(dateFrom string, dateRange []string, daysForward int) {
	log.Println("Starting reading/aggregating the results")

	reportDay := dateFrom
	reportIndex := 0

	for i, eachDay := range dateRange {

		fileList := []string{}
		var err error

		if eachDay == reportDay {

			reportIndex = i

			if verbose {
				log.Printf("Adding %d files per MSO for reporting date: %v\n", daysForward+1, reportDay)
			}
			// Adding files with the requested days before for THIS reporting day
			// Starting one day before -1 -up-to- N daysForward
			for jj := reportIndex - 1; jj <= reportIndex+daysForward; jj++ {
				if verbose {
					log.Printf("ReportDay: %s, ReportIndex: %d, DayForward: %d, jj: %d\n", reportDay, reportIndex, daysForward, jj)
					log.Println(dateRange)
				}
				err = filepath.Walk("cdw-viewership-reports/"+dateRange[jj]+"/", func(path string, f os.FileInfo, err error) error {
					if isFileToPush(path) {
						fileList = append(fileList, path)
						if verbose {
							log.Printf("Added %s for reporting date: %v\n", path, reportDay)
						}
					}
					return nil
				})
			}

			if err != nil {
				log.Println("Error walking the provided path: ", err)
			}

			// Now start processing the files to generate the aggregated reports
			filesPack := NewFilesPack(fileList)
			aggregatedReport, err := NewAggregatedReport(formatReportFilename("viewership-report", reportDay))
			if err == nil {
				aggregatedReport.ProcessFiles(filesPack, reportDay)
			} else {
				log.Printf("Error while creating aggregator: ", err)
			}

			// Next report day
			if reportIndex+1+daysAfter < len(dateRange) {
				reportDay = dateRange[reportIndex+1]
			}
		}
	}
}

func formatReportFilename(fileName, date string) string {
	return fmt.Sprintf("%s-%s.csv", fileName, date)
}

// PrintFinalReport prints the summary of app run
func PrintFinalReport(report ReportEntryList, date string, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("Aggregated final for:", date)

	reportFileName := formatReportFilename("viewership-report", date)
	reportForDate := report.Filter(date)
	if verbose {
		log.Printf("Date: %s, number of records: %d\n", date, len(reportForDate))
	}

	if testRun {
		log.Println("Saving full dump:")
		saveCSV(date+"-full-dump.csv", report)
	}

	saveCSV(reportFileName, reportForDate)
	log.Println("Saved the report in file: ", reportFileName)
}

func saveCSV(reportFileName string, reportForDate ReportEntryList) {
	out, err := os.Create(reportFileName)
	if err != nil {
		log.Println("Error creating report:", err)
		return
	}

	defer out.Close()

	writer := csv.NewWriter(out)

	writer.WriteAll(reportForDate.Convert(true, true))

	if err := writer.Error(); err != nil {
		log.Println("error writing csv:", err)
		return
	}

}

// ReportEntry struct for the aggregated repprt entry
// hh_id, ts, pg_id, pg_name, ch_num, ch_name, event, zipcode, country
// 112961,2016-07-02 23:21:58,975540,"Oklahoma News Report",3,KETA,watch,79081,USA
type ReportEntry struct {
	hh_id   string
	ts      string
	pg_id   string
	pg_name string
	ch_num  string
	ch_name string
	event   string
	zipcode string
	country string
}

// ReportEntryList list of ReportEntry
type ReportEntryList []ReportEntry

// Convert converts []ReportEntry into [][]string for csv file
func (report ReportEntryList) Convert(headerOn bool, addQuotes bool) [][]string {
	header := []string{"ts", "hh_id", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"}
	bodyAll := [][]string{}
	quotes := ""

	if headerOn {
		bodyAll = append(bodyAll, header)
	}

	if addQuotes {
		quotes = "\""
	}

	for _, entry := range report {
		bodyAll = append(bodyAll,
			[]string{
				entry.ts,
				entry.hh_id,
				entry.pg_id,
				quotes + entry.pg_name + quotes,
				entry.ch_num,
				entry.ch_name,
				entry.event,
				entry.zipcode,
				entry.country,
			})
	}
	return bodyAll
}

// Len returns the length of the list - for Sortable Interface
func (report ReportEntryList) Len() int {
	return len(report)
}

// Less returns if a[i]<a[j] - for Sortable Interface
func (report ReportEntryList) Less(i, j int) bool {
	return report[i].ts < report[j].ts
}

// Swap swaps elements i and j - for Sortable Interface
func (report ReportEntryList) Swap(i, j int) {
	report[i], report[j] = report[j], report[i]
}

// Filter returns only the entries for given date
func (report ReportEntryList) Filter(date string) ReportEntryList {
	var reportForDate ReportEntryList
	// 0123 45 67
	// 2016 06 01
	// 2016-06-01
	date = date[:4] + "-" + date[4:6] + "-" + date[6:8]

	for _, entry := range report {
		if strings.Contains(entry.ts, date) {
			reportForDate = append(reportForDate, entry)
		}
	}

	return reportForDate
}

// ReadViewershipEntries reads hh count from a single file
func ReadViewershipEntries(fileName string) []ReportEntry {
	entries := []ReportEntry{}

	entriesFile, err := os.Open(fileName)
	if err != nil {
		log.Printf("Could not open viewership file: %s, Error: %s\n", fileName, err)
		return entries
	}

	r := csv.NewReader(entriesFile)
	records, err := r.ReadAll()
	if err != nil {
		log.Printf("Could not read viewership file: %s, Error: %s\n", fileName, err)
		return entries
	}

	for i, record := range records {
		// Skipping the first line - header
		if i > 0 {
			// 	---			---			0		1		2		3			4		5			6		7			8
			// 						 "hh_id", "ts", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"
			entries = append(entries, ReportEntry{record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]})
		}
	}
	if verbose {
		log.Printf("Read: %d entries from %s \n", len(records), fileName)
	}
	return entries

}

func isFileToPush(fileName string) bool {
	return filepath.Ext(fileName) == ".csv"
}

// ReportFailedFiles prints the report of failed to download files if any
func ReportFailedFiles(failedFilesList []string) {
	if len(failedFilesList) > 0 {
		for _, key := range failedFilesList {
			log.Println("Failed downloading: ", key)
		}
	} else {
		log.Println("No failed downloads")
	}
}

func processSingleDownload(key string, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < maxAttempts; i++ {
		log.Println("Downloading: ", key)
		if downloadFile(key) && unzipAndSortFile(key) {
			if verbose {
				log.Println("Successfully downloaded: ", key)
			}
			downloadedReportChannel <- true
			return
		}

		if verbose {
			log.Println("Failed, going to sleep for: ", key)
		}
		time.Sleep(time.Duration(10) * time.Second)

	}
	failedFilesChan <- key
}

func createPath(path string) error {
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	return err
}

func downloadFile(filename string) bool {

	err := createPath(filename)
	if err != nil {
		log.Println("Could not create folder: ", filepath.Dir(filename))
		return false
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Println("Failed to create file: ", err)
		return false
	}

	defer file.Close()

	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(regionName)}))

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(filename),
		})

	if err != nil {
		log.Printf("Failed to download file: %s, Error: %s ", filename, err)
		return false
	}

	log.Println("Downloaded file ", file.Name(), numBytes, " bytes")
	return true
}

// unzipAndFilterSort unzips, sorts, and saves the original file:
// 1. unzips into memory, convert into reportList
// REMOVED 2. filters by the provided date
// 3. sorts the file
// 4. saves it
func unzipAndSortFile(fileName string) bool {
	// removing the filter part
	//date = date[:4] + "-" + date[4:6] + "-" + date[6:8]

	// 1. unzip the file
	handle, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0660)

	if err != nil {
		log.Println("Error opening gzip file: ", err)
		return false
	}

	zipReader, err := gzip.NewReader(handle)
	if err != nil {
		log.Println("Error: ", err)
		return false
	}

	defer zipReader.Close()

	// 1a. convert it into reportList and filter by date
	r := csv.NewReader(zipReader)
	records, err := r.ReadAll()
	if err != nil {
		log.Printf("Could not read viewership file: %s, Error: %s\n", fileName, err)
		return false
	}

	var entries ReportEntryList

	for i, record := range records {
		// Skipping the first line - header
		if i > 0 {
			// 	---			---			0		1		2		3			4		5			6		7			8
			// 						 "hh_id", "ts", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"
			// FILTER - if the date is for the report date:
			// Removing Fiter part
			//if strings.Contains(record[1], date) {
			entries = append(entries, ReportEntry{record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]})
			//}
		}
	}

	// 3. Sort the entries
	sort.Sort(entries)

	// 4. Save the file back
	saveCSV(strings.TrimSuffix(fileName, ".gzip"), entries)

	if verbose {
		log.Printf("Read: %d entries from %s \n", len(records), fileName)
	}

	return true

}

// unzipFile unzips and saves the .csv.gzip file into .csv file
func unzipFile(fileName string) bool {
	handle, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0660)

	if err != nil {
		log.Println("Error opening gzip file: ", err)
		return false
	}

	zipReader, err := gzip.NewReader(handle)
	if err != nil {
		log.Println("Error: ", err)
		return false
	}

	defer zipReader.Close()

	fileContents, err := ioutil.ReadAll(zipReader)

	if err != nil {
		log.Println("Error ReadAll: ", err)
		return false
	}

	err = handle.Close()
	if err != nil {
		log.Println("Error closing file: ", err)
		return false
	}

	return SaveUnzippedContent(fileName, fileContents)
}

// SaveUnzippedContent saves the unzipped content
func SaveUnzippedContent(fileName string, fileContents []byte) bool {
	unzippedFileName := strings.TrimSuffix(fileName, ".gzip")
	if verbose {
		log.Printf("Unzipping %s into %s\n", fileName, unzippedFileName)
	}

	file, err := os.Create(unzippedFileName)

	defer file.Close()

	if err != nil {
		log.Println("Failed creating unzipped file: ", err)
		return false
	}

	if _, err := file.Write(fileContents); err != nil {
		log.Println("Error writing unzipped content: ", err)
		return false
	}

	file.Sync()

	return true
}
