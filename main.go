package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var idColumn = flag.String("id", "Id", "id column name")
var outputHeaders = flag.String("headers", "Id,FirstName,LastName,Phone,Email", "output header order")
var flagDir = flag.String("dir", "", "directory containing CSVs")

// Id column name
var id string

// Struct and mutex for consolidating our records
type Data struct {
	mu      sync.Mutex
	headers map[string]string
	records map[string]map[string]string
}

func main() {
	// Parse console flags
	flag.Parse()

	// Deref the id column flag
	id = *idColumn

	// Check for the dir flag or bail out
	if *flagDir == "" {
		fmt.Println("missing -dir flag")
		os.Exit(1)
	}

	// Create an array of files to be processed
	var csvs []*csv.Reader
	files, err := ioutil.ReadDir(*flagDir)
	if err != nil {
		log.Fatal(err)
	}

	// Create a reader for each csv file
	for _, fi := range files {
		if filepath.Ext(fi.Name()) == ".csv" {
			f, err := os.Open(filepath.Join(*flagDir, fi.Name()))
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			csvs = append(csvs, csv.NewReader(f))
		}
	}

	// Initialize our request queue and our data struct
	var requests []Job
	var data = Data{
		headers: make(map[string]string),
		records: make(map[string]map[string]string),
	}

	// Create a worker for each csv - mostly unnecessary for this
	jobs := make(chan Job, len(csvs))
	// Fill our request array
	for _, csv := range csvs {
		requests = append(requests, Job{
			data: &data,
			csv:  csv,
		})
	}
	// Create the necessary workers and run them all
	go allocate(jobs, requests)
	done := make(chan bool)
	results := make(chan Result, len(requests))
	go result(results, done)
	createWorkerPool(jobs, results, len(csvs))
	<-done

	// Craft an array of our record ids so we can sort them
	keys := make([]string, 0, len(data.records))
	for k := range data.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Check if any columns were specified for the output
	// Any not specified are added in no particular order
	headers := strings.Split(*outputHeaders, ",")
	for k := range data.headers {
		if !contains(headers, k) {
			headers = append(headers, k)
		}
	}

	{
		// Write out our new csv to stdout
		writer := csv.NewWriter(os.Stdout)
		defer writer.Flush()

		// Write out our headers
		{
			err := writer.Write(headers)
			if err != nil {
				log.Fatal(err)
			}
		}

		// Write out our data
		{
			for _, k := range keys {
				line := make([]string, 0, len(data.records[k]))
				for _, column := range headers {
					line = append(line, data.records[k][column])
				}
				err := writer.Write(line)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

// Actually processes each of our CSVs
func worker(jobs chan Job, results chan Result, wg *sync.WaitGroup) {
	for job := range jobs {
		// Grab our header
		header, err := job.csv.Read()
		// No header? Can't really go on
		if err == nil {
			// Get the column index for our Id column
			idIndex := indexOf(id, header)
			// If we don't have one we have to bail out
			if idIndex > -1 {
				for _, name := range header {
					// Lock our record
					job.data.mu.Lock()
					job.data.headers[name] = name
					// Release our lock
					job.data.mu.Unlock()
				}
				// Process each line
				for {
					record, err := job.csv.Read()
					// Bail out if we reach the EOF or some other issue
					if err == io.EOF || err != nil {
						break
					}
					// Lock our record
					job.data.mu.Lock()
					// Initialize the record in the data map
					_, exists := job.data.records[record[idIndex]]
					if !exists {
						job.data.records[record[idIndex]] = make(map[string]string)
					}
					// Iterate each column and add it to the appropriate record
					for index, column := range record {
						job.data.records[record[idIndex]][header[index]] = column
					}
					// Release our lock
					job.data.mu.Unlock()
				}
			}
		}
		output := Result{job}
		results <- output
	}
	wg.Done()
}

// Creates the necessary number of workers and executes
func createWorkerPool(jobs chan Job, results chan Result, workers int) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}
	wg.Wait()
	close(results)
}

// Allocates the necessary number of jobs
func allocate(jobChan chan Job, jobs []Job) {
	if jobs != nil {
		for i := 0; i < len(jobs); i++ {
			jobs[i].id = i
			jobChan <- jobs[i]
		}
	}
	close(jobChan)
}

// Our result channel
func result(results chan Result, done chan bool) {
	done <- true
}

// Our job struct
type Job struct {
	id   int
	data *Data
	csv  *csv.Reader
}

// Our result struct
type Result struct {
	job Job
}

// Borrowed from stack overflow
func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1
}

// Borrowed from stack overflow
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
