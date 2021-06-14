package main

import (
	"github.com/spf13/afero"
	"reflect"
	"testing"
)

var TestGetTotalCountData = []struct {
	input  map[string]int
	output int
}{
	{
		map[string]int{"a": 1},
		1,
	},
	{
		map[string]int{"a": 1, "b": 2},
		3,
	},
	{
		map[string]int{"a": 1, "b": 2, "c": 96},
		99,
	},
}

func TestGetTotalCount(t *testing.T) {
	data := TestGetTotalCountData
	for i := 0; i < len(data); i++ {
		result := getTotalCount(data[i].input)
		if result != data[i].output {
			t.Errorf("expected: %d; got: %d", data[i].output,
				result)
		}
	}
}

var TestGetDistinctCountData = []struct {
	input  map[string]int
	output int
}{
	{
		map[string]int{"a": 1},
		1,
	},
	{
		map[string]int{"a": 1, "b": 2},
		2,
	},
	{
		map[string]int{"a": 1, "b": 2, "c": 96, "d": 1, "e": 472822, "f": 4627},
		6,
	},
}

func TestGetDistinctCount(t *testing.T) {
	data := TestGetDistinctCountData
	for i := 0; i < len(data); i++ {
		result := getDistinctCount(data[i].input)
		if result != data[i].output {
			t.Errorf("expected: %d; got: %d", data[i].output,
				result)
		}
	}
}

var TestGetDistinctOverlapData = []struct {
	input  [](map[string]int)
	output int
}{
	{
		[]map[string]int{
			map[string]int{"a": 1, "b": 4, "c": 5},
			map[string]int{"a": 1, "c": 2, "d": 7, "e": 1},
		},
		2,
	},
	{
		[]map[string]int{
			map[string]int{"a": 1, "b": 4, "c": 5, "e": 1, "g": 7, "h": 16},
			map[string]int{"a": 1, "c": 2, "d": 7, "e": 1, "h": 3},
		},
		4,
	},
}

func TestGetDistinctOverlap(t *testing.T) {
	data := TestGetDistinctOverlapData
	for i := 0; i < len(data); i++ {
		result := getDistinctOverlap(data[i].input[0], data[i].input[1])
		if result != data[i].output {
			t.Errorf("expected: %d; got: %d", data[i].output,
				result)
		}
	}
}

var TestGetTotalOverlapData = []struct {
	input  [](map[string]int)
	output int
}{
	{
		[]map[string]int{
			map[string]int{"a": 1, "b": 4, "c": 5},
			map[string]int{"a": 1, "c": 2, "d": 7, "e": 1},
		},
		9,
	},
	{
		[]map[string]int{
			map[string]int{"a": 1, "b": 4, "c": 5, "e": 1, "g": 7, "h": 16},
			map[string]int{"a": 1, "c": 2, "d": 7, "e": 1, "h": 3},
		},
		30,
	},
}

func TestGetTotalOverlap(t *testing.T) {
	data := TestGetTotalOverlapData
	for i := 0; i < len(data); i++ {
		result := getTotalOverlap(data[i].input[0], data[i].input[1])
		if result != data[i].output {
			t.Errorf("expected: %d; got: %d", data[i].output,
				result)
		}
	}
}

type testfile struct {
	filename string
	filedata []byte
}

var TestGetEntriesMapData = []struct {
	input  testfile
	output map[string]int
}{
	{
		input: testfile{
			filename: "file1.txt",
			filedata: []byte("udprn\n30433784\n08034283\n71842328\n51357306\n20217152\n91285130\n77055067\n03258293\n08034283\n61337293\n"),
		},
		output: map[string]int{"30433784": 1, "08034283": 2, "71842328": 1, "51357306": 1, "20217152": 1, "91285130": 1, "77055067": 1, "03258293": 1, "61337293": 1},
	},
	{
		input: testfile{
			filename: "file2.txt",
			filedata: []byte("udprn\n\"\"\n83003979\n81941485\n67732207\n97403400\n88257317\n94765729\n23842754\n\"\"\n51451818\n\"\"\n96742547\n\"\"\n00885975\n99904844\n59341227\n43405227\n04188077\n79055373\n57408241\n84227404\n21697870\n50458445\n50057568\n39232320\n"),
		},
		output: map[string]int{"83003979": 1, "81941485": 1, "67732207": 1, "97403400": 1, "88257317": 1, "94765729": 1, "23842754": 1, "51451818": 1, "96742547": 1, "00885975": 1, "99904844": 1, "59341227": 1, "43405227": 1, "04188077": 1, "79055373": 1, "57408241": 1, "84227404": 1, "21697870": 1, "50458445": 1, "50057568": 1, "39232320": 1},
	},
}

func TestGetEntriesMap(t *testing.T) {
	data := TestGetEntriesMapData
	for i := 0; i < len(data); i++ {
		// Create file in virtual filesystem; inserting test data
		afero.WriteFile(afs, data[i].input.filename, data[i].input.filedata, 0755)
		// Perhaps further work here would add tests for different kinds of errors
		result, _ := getEntriesMap(data[i].input.filename)
		// Seems like a reasonable use-case for reflect
		if !reflect.DeepEqual(result, data[i].output) {
			t.Errorf("%dth map element in test set for TestGetEntriesMap does not match expected output",
				i)
		}
	}

}
