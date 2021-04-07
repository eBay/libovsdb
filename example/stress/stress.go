package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"

	"github.com/ebay/libovsdb"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to this file")
var memprofile = flag.String("memoryprofile", "", "write memory profile to this file")
var api = flag.String("api", "", "api to use: [legacy (default), native]")
var nins = flag.Int("ninserts", 100, "insert this number of elements in the database")
var verbose = flag.Bool("verbose", false, "Be verbose")
var connection = flag.String("ovsdb", "unix:/var/run/openvswitch/db.sock", "OVSDB connection string")

var (
	cache    map[string]map[string]interface{}
	rootUUID string
	summary  = map[string]int{
		"deletions":  0,
		"insertions": 0,
		"listings":   0,
	}
)

func list() {
	ovs, err := libovsdb.Connect(*connection, "Open_vSwitch", nil)
	if err != nil {
		log.Fatal(err)
	}
	final, err := ovs.MonitorAll("")
	if err != nil {
		log.Fatal(err)
	}
	populateCache(ovs, *final)
}
func run() {
	ovs, err := libovsdb.Connect(*connection, "Open_vSwitch", nil)
	if err != nil {
		log.Fatal(err)
	}
	initial, _ := ovs.MonitorAll("")
	if *verbose {
		fmt.Printf("initial : %v\n\n", initial)
	}

	// Get root UUID
OUTER:
	for table, update := range initial.Updates {
		if table == "Open_vSwitch" {
			for uuid := range update.Rows {
				rootUUID = uuid
				if *verbose {
					fmt.Printf("rootUUID is %v", rootUUID)
				}
				break OUTER
			}
		}
	}

	// Remove all existing bridges
	for table, update := range initial.Updates {
		if table == "Bridge" {
			for uuid := range update.Rows {
				deleteBridge(ovs, uuid)
			}
		}
	}

	for i := 0; i < *nins; i++ {
		createBridge(ovs, i)
	}
}

func transact(ovs *libovsdb.OvsdbClient, operations []libovsdb.Operation) (ok bool, uuid string) {
	reply, _ := ovs.Transact(operations...)

	if len(reply) < len(operations) {
		fmt.Println("Number of Replies should be atleast equal to number of Operations")
	}
	ok = true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			fmt.Println("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			fmt.Println("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	uuid = reply[0].UUID.GoUUID
	return
}

func populateCache(ovs *libovsdb.OvsdbClient, updates libovsdb.TableUpdates) {
	cache = make(map[string]map[string]interface{})
	for table, tableUpdate := range updates.Updates {
		if _, ok := cache[table]; !ok {
			cache[table] = make(map[string]interface{})
		}
		for uuid, row := range tableUpdate.Rows {
			empty := libovsdb.Row{}
			if !reflect.DeepEqual(row.New, empty) {
				if *api == "native" {
					rowData, err := ovs.API.GetRowData(table, &row.New)
					if err != nil {
						log.Fatal(err)
					}

					cache[table][uuid] = rowData

				} else {
					cache[table][uuid] = row.New
				}
				summary["listings"]++
			} else {
				delete(cache[table], uuid)
			}
		}
	}
}

func deleteBridge(ovs *libovsdb.OvsdbClient, uuid string) {
	var err error
	var mutation []interface{}
	var delCondition []interface{}
	var mutCondition []interface{}

	if *api == "native" {
		delCondition, err = ovs.API.NewCondition("Bridge", "_uuid", "==", uuid)
		if err != nil {
			log.Fatal(err)
		}
		mutation, err = ovs.API.NewMutation("Open_vSwitch", "bridges", "delete", []string{uuid})
		if err != nil {
			log.Fatal(err)
		}
		mutCondition, err = ovs.API.NewMutation("Open_vSwitch", "_uuid", "==", rootUUID)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		delCondition = libovsdb.NewCondition("_uuid", "==", libovsdb.UUID{GoUUID: uuid})

		mutateUUID := []libovsdb.UUID{{GoUUID: uuid}}
		mutateSet, _ := libovsdb.NewOvsSet(mutateUUID)
		mutation = libovsdb.NewMutation("bridges", "delete", mutateSet)
		// hacked Condition till we get Monitor / Select working
		mutCondition = libovsdb.NewCondition("_uuid", "==", libovsdb.UUID{GoUUID: rootUUID})
	}

	deleteOp := libovsdb.Operation{
		Op:    "delete",
		Table: "Bridge",
		Where: []interface{}{delCondition},
	}
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{mutCondition},
	}

	operations := []libovsdb.Operation{deleteOp, mutateOp}
	ok, _ := transact(ovs, operations)
	if ok {
		summary["deletions"]++
		if *verbose {
			fmt.Println("Bridge Deletion Successful : ", uuid)
		}
	}
}

func createBridge(ovs *libovsdb.OvsdbClient, iter int) {
	bridge := make(map[string]interface{})
	namedUUID := "gopher"
	bridgeName := fmt.Sprintf("bridge-%d", iter)
	if *api == "native" {
		nbridge := make(map[string]interface{})
		var err error
		datapathID := []string{"blablabla"}
		otherConfig := map[string]string{
			"foo":  "bar",
			"fake": "config",
		}
		externalIds := map[string]string{
			"key1": "val1",
			"key2": "val2",
		}
		nbridge["name"] = bridgeName
		nbridge["other_config"] = otherConfig
		bridge["datapath_id"] = datapathID
		nbridge["external_ids"] = externalIds

		bridge, err = ovs.API.NewRow("Bridge", nbridge)
		if err != nil {
			log.Fatal(err)
		}

	} else {
		datapathID, _ := libovsdb.NewOvsSet([]string{"blablabla"})
		otherConfig, _ := libovsdb.NewOvsMap(map[string]string{
			"foo":  "bar",
			"fake": "config",
		})
		externalIds, _ := libovsdb.NewOvsMap(map[string]string{
			"key1": "val1",
			"key2": "val2",
		})
		bridge["name"] = bridgeName
		bridge["other_config"] = otherConfig
		bridge["datapath_id"] = datapathID
		bridge["external_ids"] = externalIds
	}

	// simple insert operation
	insertOp := libovsdb.Operation{
		Op:       "insert",
		Table:    "Bridge",
		Row:      bridge,
		UUIDName: namedUUID,
	}

	var mutation []interface{}
	var condition []interface{}
	var err error

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	if *api == "native" {
		// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
		mutation, err = ovs.API.NewMutation("Open_vSwitch", "bridges", "insert", []string{namedUUID})
		if err != nil {
			log.Fatalf("Mutation Error: %s", err.Error())
		}
		condition, err = ovs.API.NewCondition("Open_vSwitch", "_uuid", "==", rootUUID)
		if err != nil {
			log.Fatalf("Condition Error: %s", err.Error())
		}
	} else {
		uuidParameter := libovsdb.UUID{GoUUID: rootUUID}
		mutateUUID := []libovsdb.UUID{{GoUUID: namedUUID}}
		mutateSet, _ := libovsdb.NewOvsSet(mutateUUID)
		mutation = libovsdb.NewMutation("bridges", "insert", mutateSet)
		condition = libovsdb.NewCondition("_uuid", "==", uuidParameter)
	}

	// simple mutate operation
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     "Open_vSwitch",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []libovsdb.Operation{insertOp, mutateOp}
	ok, uuid := transact(ovs, operations)
	if ok {
		summary["insertions"]++
		if *verbose {
			fmt.Println("Bridge Addition Successful : ", uuid)
		}
	}
}
func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	run()
	list()

	fmt.Printf("Summary:\n")
	fmt.Printf("\tInsertions: %d\n", summary["insertions"])
	fmt.Printf("\tDeletions: %d\n", summary["deletions"])
	fmt.Printf("\tLintings: %d\n", summary["listings"])

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
