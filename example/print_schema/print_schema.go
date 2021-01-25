package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/ebay/libovsdb"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Print schema information:\n")
	fmt.Fprintf(os.Stderr, "\tprint_schemas OVS_SCHEMA\n")
}

func main() {
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(2)
	}

	schemaFile, err := os.Open(flag.Args()[0])
	if err != nil {
		log.Fatal(err)
	}
	defer schemaFile.Close()

	schemaBytes, err := ioutil.ReadAll(schemaFile)
	if err != nil {
		log.Fatal(err)
	}

	schema := libovsdb.DatabaseSchema{}
	//if err := schema.Unmarshal(schemaBytes); err != nil {
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		log.Fatal(err)
	}
	schema.Print(os.Stdout)
}
