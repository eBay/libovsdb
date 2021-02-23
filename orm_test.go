package libovsdb

import (
	"encoding/json"
	"reflect"
	"testing"
)

type ormTestType struct {
	AString             string            `ovs:"aString"`
	ASet                []string          `ovs:"aSet"`
	ASingleSet          []string          `ovs:"aSingleSet"`
	AUUIDSet            []string          `ovs:"aUUIDSet"`
	AUUID               string            `ovs:"aUUID"`
	AIntSet             []int             `ovs:"aIntSet"`
	AFloat              float64           `ovs:"aFloat"`
	AFloatSet           []float64         `ovs:"aFloatSet"`
	YetAnotherStringSet []string          `ovs:"aEmptySet"`
	AEnum               string            `ovs:"aEnum"`
	AMap                map[string]string `ovs:"aMap"`
	NonTagged           string
}

var expected = ormTestType{
	AString:             aString,
	ASet:                aSet,
	ASingleSet:          []string{aString},
	AUUIDSet:            aUUIDSet,
	AUUID:               aUUID0,
	AIntSet:             aIntSet,
	AFloat:              aFloat,
	AFloatSet:           aFloatSet,
	YetAnotherStringSet: []string{},
	AEnum:               aEnum,
	AMap:                aMap,
	NonTagged:           "something",
}

func TestORMGetData(t *testing.T) {
	ovsRow := getOvsTestRow()
	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	api := ORMAPI{schema: &schema}
	orm := ormTestType{
		NonTagged: "something",
	}
	err := api.GetRowData("TestTable", &ovsRow, &orm)
	/*End code under test*/

	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(orm, expected) {
		t.Errorf("Failed to correctly extract ORM native value, expected %v, got %v\n",
			expected, orm)
	}
}

func TestORMNewRow(t *testing.T) {
	ovsRow := getOvsTestRow()
	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	api := ORMAPI{schema: &schema}
	orm := ormTestType{
		AString:             aString,
		ASet:                aSet,
		ASingleSet:          []string{aString},
		AUUIDSet:            aUUIDSet,
		AUUID:               aUUID0,
		AIntSet:             aIntSet,
		AFloat:              aFloat,
		AFloatSet:           aFloatSet,
		YetAnotherStringSet: aEmptySet,
		AEnum:               aEnum,
		AMap:                aMap,
		NonTagged:           "something",
	}
	row, err := api.NewRow("TestTable", &orm)
	/*End code under test*/

	if err != nil {
		t.Error(err)
	}
	for k := range row {
		if k == "aSingleSet" {
			uss1, _ := NewOvsSet([]string{aString})
			if !reflect.DeepEqual(row[k], uss1) {
				t.Errorf("Failed to convert to ovs. Key %s", k)
				t.Logf("value: %v\n", expectedOvs(row[k]))
				t.Logf("expected : %v\n", uss1)
			}

		} else {
			if !reflect.DeepEqual(expectedOvs(row[k]), ovsRow.Fields[k]) {
				t.Errorf("Failed to convert to ovs. Key %s", k)
				t.Logf("value: %v\n", expectedOvs(row[k]))
				t.Logf("expected : %v\n", ovsRow.Fields[k])
			}
		}
	}
}
