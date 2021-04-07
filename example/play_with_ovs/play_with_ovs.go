package main

import (
	"fmt"
	"log"

	"github.com/ebay/libovsdb"
)

// Silly game that detects creation of Bridge named "stop" and exits
// Just a demonstration of how an app can use libovsdb library to configure and manage OVS
const (
	bridgeTable = "Bridge"
	ovsTable    = "Open_vSwitch"
)

var quit chan bool
var update chan libovsdb.Row
var rootUUID string

func play(ovs *libovsdb.OvsdbClient) {
	go processInput(ovs)
	for {
		select {
		case row := <-update:
			rowData, err := ovs.API.GetRowData(bridgeTable, &row)
			if err != nil {
				fmt.Println("ERROR getting Bridge Data", err)
			}
			if _, ok := rowData["name"]; ok {
				name := rowData["name"].(string)
				if name == "stop" {
					fmt.Println("Bridge stop detected : ", rowData["_uuid"])
					ovs.Disconnect()
					quit <- true
				}
			}
		}
	}
}

func createBridge(ovs *libovsdb.OvsdbClient, bridgeName string) {
	namedUUID := "gopher"
	// bridge row to insert
	bridge := make(map[string]interface{})
	bridge["name"] = bridgeName
	bridge["external_ids"] = map[string]string{"purpose": "fun"}

	brow, err := ovs.API.NewRow(bridgeTable, bridge)
	if err != nil {
		log.Fatalf("Row Error: %s", err.Error())
	}
	// simple insert operation
	insertOp := libovsdb.Operation{
		Op:       "insert",
		Table:    bridgeTable,
		Row:      brow,
		UUIDName: namedUUID,
	}

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	mutation, err := ovs.API.NewMutation(ovsTable, "bridges", "insert", []string{namedUUID})
	if err != nil {
		log.Fatalf("Mutation Error: %s", err.Error())
	}
	condition, err := ovs.API.NewCondition(ovsTable, "_uuid", "==", rootUUID)
	if err != nil {
		log.Fatalf("Condition Error: %s", err.Error())
	}

	// simple mutate operation
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     ovsTable,
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []libovsdb.Operation{insertOp, mutateOp}
	reply, err := ovs.Transact(operations...)
	if err != nil {
		log.Fatal(err)
	}

	if len(reply) < len(operations) {
		fmt.Println("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			fmt.Println("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			fmt.Println("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUUID)
	}
}

func processInput(ovs *libovsdb.OvsdbClient) {
	for {
		fmt.Printf("\n Enter a Bridge Name : ")
		var bridgeName string
		fmt.Scanf("%s", &bridgeName)
		createBridge(ovs, bridgeName)
	}
}

func main() {
	quit = make(chan bool)
	update = make(chan libovsdb.Row)
	// By default libovsdb connects to 127.0.0.0:6400.
	ovs, err := libovsdb.Connect("tcp:", "Open_vSwitch", nil)

	// If you prefer to connect to OVS in a specific location :
	// ovs, err := libovsdb.Connect("tcp:192.168.56.101:6640", nil)

	if err != nil {
		log.Fatal("Unable to Connect ", err)
	}

	ovs.Cache.AddEventHandler(&libovsdb.EventHandlerFuncs{
		AddFunc: func(table string, row libovsdb.Row) {
			if table == bridgeTable {
				update <- row
			}
		},
	})

	err = ovs.MonitorAll("")
	if err != nil {
		log.Fatal(err)
	}

	rootUUID = ovs.Cache.Table(ovsTable).Rows()[0]

	fmt.Println(`Silly game of stopping this app when a Bridge with name "stop" is monitored !`)
	go play(ovs)
	<-quit
}
