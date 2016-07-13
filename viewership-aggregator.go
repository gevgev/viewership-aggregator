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
	version     = "0.1"
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

	verbose bool
	appName string

	failedFilesChan         chan string
	downloadedReportChannel chan bool

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
	flagHelp := flag.Bool("h", false, "Help")

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

		verbose = *flagVerbose
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -r <aws_region> -b <s3_bucket_name> --from <date> --to <date> -m <mso-list-file-name> -M <max_retry>\n", appName)
	flag.Usage()
	os.Exit(-1)
}

func PrintParams() {
	log.Printf("Provided: -r: %s, -b: %s, --from: %v, --to: %v, -m %s, -M %d, -v: %v\n",
		regionName,
		bucketName,
		dateFrom,
		dateTo,
		msoListFilename,
		maxAttempts,
		verbose,
	)

}

type MsoType struct {
	Code string
	Name string
}

// Read the list of MSO's and initialize the lookup map and array
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

// path per mso
func formatPrefix(path, msoCode string) string {
	return fmt.Sprintf("%s/%s/delta/", path, msoCode)
}

// converts/breaks the "20160601" string into yy, mm, dd
func convertToDateParts(dtStr string) (yy, mm, dd int) {
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

	i, err = strconv.Atoi(dtStr[7:])
	if err != nil {
		return yy, mm, dd
	}
	dd = i
	return yy, mm, dd
}

// a list of strings for each date in range to lookup
func getDateRangeRegEx(dateFrom, dateTo string) []string {

	regExpStr := []string{}
	//'20160630'
	yy, mm, dd := convertToDateParts(dateFrom)
	dtFrom := time.Date(yy, time.Month(mm), dd, 0, 0, 0, 0, time.UTC)
	if verbose {
		log.Println("From:", dtFrom.String())
	}

	yy, mm, dd = convertToDateParts(dateTo)
	dtTo := time.Date(yy, time.Month(mm), dd, 0, 0, 0, 0, time.UTC)
	if verbose {
		log.Println("To:", dtTo.String())
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

	dateRangeRegexStr := getDateRangeRegEx(dateFrom, dateTo)

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

			for _, eachDate := range dateRangeRegexStr {
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

	ReportFailedFiles(failedFilesList)

	GenerateDailyAggregates(dateRangeRegexStr)

	log.Printf("Processed %d MSO's, %d days, in %v\n", len(msoList), len(dateRangeRegexStr), time.Since(startTime))
}

// GenerateDailyAggregates will walk through day/mso files, and will aggregate/sort them
func GenerateDailyAggregates(dateRange []string) {
	log.Println("Starting reading/aggregating the results")
	var wg sync.WaitGroup

	for _, eachDay := range dateRange {

		fileList := []string{}
		var err error
		err = filepath.Walk("cdw-viewership-reports/"+eachDay+"/", func(path string, f os.FileInfo, err error) error {
			if isFileToPush(path) {
				fileList = append(fileList, path)
			}
			return nil
		})

		if err != nil {
			log.Println("Error walking the provided path: ", err)
		}

		var report ReportEntryList

		for _, file := range fileList {
			if isFileToPush(file) {
				if verbose {
					log.Println("Reading: ", file)
				}
				ss := ReadViewershipEntries(file)
				before := len(report)
				report = append(report, ss...)
				if verbose {
					log.Printf("Appending %d records from file %s. Before: %d, now: %d records\n", len(ss), file, before, len(report))
				}
			}
		}

		sort.Sort(report)
		wg.Add(1)
		go PrintFinalReport(report, eachDay, &wg)
	}
	wg.Wait()

}

func formatReportFilename(fileName, date string) string {
	return fmt.Sprintf("%s-%s.csv", fileName, date)
}

func PrintFinalReport(report ReportEntryList, date string, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("Aggregated final for:", date)

	reportFileName := formatReportFilename("viewership-report", date)

	out, err := os.Create(reportFileName)
	if err != nil {
		log.Println("Error creating report:", err)
		return
	}

	defer out.Close()

	writer := csv.NewWriter(out)

	reportForDate := report.Filter(date)
	if verbose {
		log.Printf("Date: %s, number of records: %d\n", date, len(reportForDate))
	}

	writer.WriteAll(reportForDate.Convert())

	if err := writer.Error(); err != nil {
		log.Println("error writing csv:", err)
		return
	}

	log.Println("Saved the report in file: ", reportFileName)
}

// hh_id, ts, pg_id, pg_name, ch_num, ch_name, event, zipcode, country
// 112961,2016-07-02 23:21:58,975540,"Oklahoma News Report",3,KETA,watch,79081,USA

//aggregated reported entry
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

type ReportEntryList []ReportEntry

// convert []ReportEntry into [][]string for csv file
func (report ReportEntryList) Convert() [][]string {
	header := []string{"ts", "hh_id", "pg_id", "pg_name", "ch_num", "ch_name", "event", "zipcode", "country"}
	bodyAll := [][]string{}

	bodyAll = append(bodyAll, header)

	for _, entry := range report {
		bodyAll = append(bodyAll,
			[]string{
				entry.ts,
				entry.hh_id,
				entry.pg_id,
				`"` + entry.pg_name + `"`,
				entry.ch_num,
				entry.ch_name,
				entry.event,
				entry.zipcode,
				entry.country,
			})
	}
	return bodyAll
}

func (list ReportEntryList) Len() int {
	return len(list)
}

func (list ReportEntryList) Less(i, j int) bool {
	return list[i].ts < list[j].ts
}

func (list ReportEntryList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

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

// Read hh count from a single file
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
		if downloadFile(key) && unzipFile(key) {
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
