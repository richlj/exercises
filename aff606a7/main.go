package main

import (
	"bufio"
	"fmt"
	"github.com/spf13/afero"
	"log"
	"os"
	"strings"
)

var (
	// For mocking filesystem for tests
	fs  afero.Fs     = afero.NewOsFs()
	afs *afero.Afero = &afero.Afero{Fs: fs}

	headerName = "udprn"

	headerNotFoundErr = "header '%s' not found in input file '%s'"
	multipleHeaderErr = "file '%s' has multiple colums with header '%s'"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("insufficient number of arguments; require: %s ${file1} ${file2}",
			os.Args[0])
	}

	map1, err := getEntriesMap(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	map2, err := getEntriesMap(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total count for %s:\t%d\n", os.Args[1],
		getTotalCount(map1))
	fmt.Printf("Total count for %s:\t%d\n\n", os.Args[2],
		getTotalCount(map2))

	fmt.Printf("Distinct count for %s:\t%d\n", os.Args[1],
		getDistinctCount(map1))
	fmt.Printf("Distinct count for %s:\t%d\n\n", os.Args[2],
		getDistinctCount(map2))

	fmt.Printf("Total overlap for %s and %s:\t\t%d\n", os.Args[1],
		os.Args[2], getTotalOverlap(map1, map2))
	fmt.Printf("Distinct overlap for %s and %s:\t%d\n", os.Args[1],
		os.Args[2], getDistinctOverlap(map1, map2))
}

/*
	total count: total quantity of entries in slice
	distinct count: total quantity of entries in slice that are distinct

	total overlap: total quantity of entries that exist and which are in
	both files
	distinct overlap: total count of entries that exist in both files

*/

// getEntriesMap returns a map of the data in lines for the file with the
// supplied filename. Seeing as we won't need to sort it, this will be the
// most memory and computationally-efficient format for the operations that
// we will then use for this data
func getEntriesMap(a string) (map[string]int, error) {
	result := make(map[string]int)
	file, err := afs.Open(a)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	for s, header, i := bufio.NewScanner(file), true, -1; s.Scan(); header = false {
		// Find correct row if len() > 1 and check for ambiguities
		if header {
			for k, v := range strings.Split(s.Text(), ",") {
				if v == headerName {
					if i < 0 {
						i = k
					} else {
						file.Close()
						return nil,
							fmt.Errorf(multipleHeaderErr,
								a, headerName)
					}
				}
			}
			if i < 0 {
				return nil, fmt.Errorf(headerNotFoundErr, headerName, a)
			}
			continue // don't want to add header to result
		}
		if v := strings.Split(s.Text(), ","); len(v) > i {
			// Ignoring what are presumably null values (discuss?)
			if v[i] != "\"\"" {
				result[v[i]]++
			}
		}
	}
	return result, nil
}

// getTotalCount measures the sum of the values in the supplied map which
// contains data about lines in a file
func getTotalCount(a map[string]int) int {
	var result int
	for _, v := range a {
		result += v
	}
	return result
}

// getDistinctCount returns the count of entries of the supplied map about
// instances of values of lines in a file thereby providing the number of
// distinct occurences of values
func getDistinctCount(a map[string]int) int {
	return len(a)
}

// getDistinctOverlap counts the number of values that are found to appear in
// both files
func getDistinctOverlap(a, b map[string]int) int {
	var result int
	for k := range a {
		if _, ok := b[k]; ok {
			result++
		}
	}
	return result
}

// getTotalOverlap returns the sum of the number of occurences of values that
// appear in both files
func getTotalOverlap(a, b map[string]int) int {
	var result int
	for k, v := range a {
		if val, ok := b[k]; ok {
			result += (v + val)
		}
	}
	return result
}
