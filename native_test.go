package libovsdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestGetData(t *testing.T) {
	ovsRow := getOvsTestRow()

	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	nf := NativeAPI{schema: &schema}
	data, err := nf.GetRowData("TestTable", &ovsRow)
	if err != nil {
		t.Error(err)
	}
	/* End: code under test */

	if len(data) != len(ovsRow.Fields) {
		t.Errorf("wrong length %d", len(data))
	}

	// Verify I can cast the content of data to native types
	if v, ok := data["aSet"].([]string); !ok || !reflect.DeepEqual(v, aSet) {
		t.Errorf("invalid set value %v", v)
	}
	if v, ok := data["aMap"].(map[string]string); !ok || !reflect.DeepEqual(v, aMap) {
		t.Errorf("invalid map value %v", v)
	}
	if v, ok := data["aUUIDSet"].([]string); !ok || !reflect.DeepEqual(v, aUUIDSet) {
		t.Errorf("invalid uuidset value %v", v)
	}
	if v, ok := data["aUUID"].(string); !ok || !reflect.DeepEqual(v, aUUID0) {
		t.Errorf("invalid uuidvalue %v", v)
	}
	if v, ok := data["aIntSet"].([]int); !ok || !reflect.DeepEqual(v, aIntSet) {
		t.Errorf("invalid integer set %v", v)
	}
}

func TestNewRow(t *testing.T) {
	ovsRow := getOvsTestRow()

	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}
	nf := NativeAPI{schema: &schema}
	row, err := nf.NewRow("TestTable", getNativeTestMap())
	if err != nil {
		t.Error(err)
	}

	for k := range row {
		if !reflect.DeepEqual(expectedOvs(row[k]), ovsRow.Fields[k]) {
			t.Errorf("Failed to convert to ovs. Key %s", k)
			fmt.Printf("value: %v\n", expectedOvs(row[k]))
			fmt.Printf("expected : %v\n", ovsRow.Fields[k])
		}

	}
}
