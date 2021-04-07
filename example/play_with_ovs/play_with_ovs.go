package main

import (
	"fmt"
	"os"
	"reflect"

	"github.com/ebay/libovsdb"
)

// Silly game that detects creation of Bridge named "stop" and exits
// Just a demonstration of how an app can use libovsdb library to configure and manage OVS
const (
	bridgeTable = "Bridge"
	ovsDb       = "Open_vSwitch"
	ovsTable    = ovsDb
)

var quit chan bool
var update chan *libovsdb.TableUpdates
var cache map[string]map[string]libovsdb.Row

func play(ovs *libovsdb.OvsdbClient) {
	go processInput(ovs)
	for {
		select {
		case currUpdate := <-update:
			for table, tableUpdate := range currUpdate.Updates {
				if table == bridgeTable {
					for uuid, row := range tableUpdate.Rows {
						rowData, err := ovs.API.GetRowData(bridgeTable, &row.New)
						if err != nil {
							fmt.Println("ERROR getting Bridge Data", err)
						}
						if _, ok := rowData["name"]; ok {
							name := rowData["name"].(string)
							if name == "stop" {
								fmt.Println("Bridge stop detected : ", uuid)
								ovs.Disconnect()
								quit <- true
							}
						}
					}
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
		fmt.Printf("Row Error: %s", err.Error())
		os.Exit(1)

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
		fmt.Printf("Mutation Error: %s", err.Error())
		os.Exit(1)
	}
	condition, err := ovs.API.NewCondition(ovsTable, "_uuid", "==", getRootUUID())
	if err != nil {
		fmt.Printf("Condition Error: %s", err.Error())
		os.Exit(1)
	}

	// simple mutate operation
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     "ovsTable",
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []libovsdb.Operation{insertOp, mutateOp}
	reply, _ := ovs.Transact(operations...)

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

func getRootUUID() string {
	for uuid := range cache[ovsTable] {
		return uuid
	}
	return ""
}

func populateCache(updates libovsdb.TableUpdates) {
	for table, tableUpdate := range updates.Updates {
		if _, ok := cache[table]; !ok {
			cache[table] = make(map[string]libovsdb.Row)

		}
		for uuid, row := range tableUpdate.Rows {
			empty := libovsdb.Row{}
			if !reflect.DeepEqual(row.New, empty) {
				cache[table][uuid] = row.New
			} else {
				delete(cache[table], uuid)
			}
		}
	}
}

func main() {
	quit = make(chan bool)
	update = make(chan *libovsdb.TableUpdates)
	cache = make(map[string]map[string]libovsdb.Row)

	// By default libovsdb connects to 127.0.0.0:6400.
	ovs, err := libovsdb.Connect("tcp:", "Open_vSwitch", nil)

	// If you prefer to connect to OVS in a specific location :
	// ovs, err := libovsdb.Connect("tcp:192.168.56.101:6640", nil)

	if err != nil {
		fmt.Println("Unable to Connect ", err)
		os.Exit(1)
	}
	var notifier myNotifier
	ovs.Register(notifier)

	initial, _ := ovs.MonitorAll("")
	populateCache(*initial)

	fmt.Println(`Silly game of stopping this app when a Bridge with name "stop" is monitored !`)
	go play(ovs)
	<-quit
}

type myNotifier struct {
}

func (n myNotifier) Update(context interface{}, tableUpdates libovsdb.TableUpdates) {
	populateCache(tableUpdates)
	update <- &tableUpdates
}
func (n myNotifier) Locked([]interface{}) {
}
func (n myNotifier) Stolen([]interface{}) {
}
func (n myNotifier) Echo([]interface{}) {
}
func (n myNotifier) Disconnected() {
}
