package libovsdb

import (
	"encoding/json"
	"fmt"
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
	// Test selected cols
	row2, err := api.NewRow("TestTable", &orm, "aSet", "aMap")
	if err != nil {
		t.Error(err)
	}
	if len(row2) != 2 {
		t.Errorf("Expected row to be as long as the list of given columns")
	}

	// Test selected cols
	row3, err := api.NewRow("TestTable", &orm, "nonexisting")
	if err != nil {
		t.Error(err)
	}
	if len(row3) != 0 {
		t.Errorf("Expected row to be as long as the list of valid given columns")
	}
}

func TestORMCondition(t *testing.T) {

	var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "indexes": [["name"],["composed_1","composed_2"]],
      "columns": {
        "name": {
          "type": "string"
        },
        "composed_1": {
          "type": {
            "key": "string"
          }
        },
        "composed_2": {
          "type": {
            "key": "string"
          }
        },
        "config": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
    }
  }
}`)
	type testType struct {
		ID     string            `ovs:"_uuid"`
		MyName string            `ovs:"name"`
		Config map[string]string `ovs:"config"`
		Comp1  string            `ovs:"composed_1"`
		Comp2  string            `ovs:"composed_2"`
	}

	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Fatal(err)
	}
	api := ORMAPI{schema: &schema}

	type Test struct {
		name     string
		obj      testType
		expected []interface{}
		index    []string
		err      bool
	}
	tests := []Test{
		{
			name: "simple index",
			obj: testType{
				MyName: "foo",
			},
			expected: []interface{}{[]interface{}{"name", "==", "foo"}},
			index:    []string{},
			err:      false,
		},
		{
			name: "UUID",
			obj: testType{
				ID:     aUUID0,
				MyName: "foo",
			},
			expected: []interface{}{[]interface{}{"_uuid", "==", UUID{GoUUID: aUUID0}}},
			index:    []string{},
			err:      false,
		},
		{
			name: "specify index",
			obj: testType{
				ID:     aUUID0,
				MyName: "foo",
			},
			expected: []interface{}{[]interface{}{"name", "==", "foo"}},
			index:    []string{"name"},
			err:      false,
		},
		{
			name: "complex index",
			obj: testType{
				Comp1: "foo",
				Comp2: "bar",
			},
			expected: []interface{}{[]interface{}{"composed_1", "==", "foo"},
				[]interface{}{"composed_2", "==", "bar"}},
			index: []string{},
			err:   false,
		},
		{
			name: "first index",
			obj: testType{
				MyName: "something",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			expected: []interface{}{[]interface{}{"name", "==", "something"}},
			index:    []string{},
			err:      false,
		},
		{
			name: "Error: None",
			obj: testType{
				Config: map[string]string{"foo": "bar"},
			},
			index: []string{},
			err:   true,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("NewCondition_%s", test.name), func(t *testing.T) {
			conds, err := api.NewCondition("TestTable", &test.obj, test.index...)
			if test.err {
				if err == nil {
					t.Errorf("Expected an error but got none")
				}
			} else {
				if err != nil {
					t.Error(err)
				}
			}

			if !reflect.DeepEqual(conds, test.expected) {
				t.Errorf("Wrong condition, expected %v (%s), got %v (%s)",
					test.expected,
					reflect.TypeOf(test.expected),
					conds,
					reflect.TypeOf(conds))
			}
		})
	}
}

func TestORMEqual(t *testing.T) {

	var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "indexes": [["name"],["composed_1","composed_2"]],
      "columns": {
        "name": {
          "type": "string"
        },
        "composed_1": {
          "type": {
            "key": "string"
          }
        },
        "composed_2": {
          "type": {
            "key": "string"
          }
        },
        "int1": {
          "type": {
            "key": "integer"
          }
        },
        "int2": {
          "type": {
            "key": "integer"
          }
        },
        "config": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
    }
  }
}`)
	type testType struct {
		ID     string            `ovs:"_uuid"`
		MyName string            `ovs:"name"`
		Config map[string]string `ovs:"config"`
		Comp1  string            `ovs:"composed_1"`
		Comp2  string            `ovs:"composed_2"`
		Int1   int               `ovs:"int1"`
		Int2   int               `ovs:"int2"`
	}

	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Fatal(err)
	}
	api := ORMAPI{schema: &schema}

	type Test struct {
		name     string
		obj1     testType
		obj2     testType
		expected bool
		indexes  []string
	}
	tests := []Test{
		{
			name: "same simple index",
			obj1: testType{
				MyName: "foo",
			},
			obj2: testType{
				MyName: "foo",
			},
			expected: true,
			indexes:  []string{},
		},
		{
			name: "diff simple index",
			obj1: testType{
				MyName: "foo",
			},
			obj2: testType{
				MyName: "bar",
			},
			expected: false,
			indexes:  []string{},
		},
		{
			name: "same uuid",
			obj1: testType{
				ID:     aUUID0,
				MyName: "foo",
			},
			obj2: testType{
				ID:     aUUID0,
				MyName: "bar",
			},
			expected: true,
			indexes:  []string{},
		},
		{
			name: "diff uuid",
			obj1: testType{
				ID:     aUUID0,
				MyName: "foo",
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "bar",
			},
			expected: false,
			indexes:  []string{},
		},
		{
			name: "same complex_index",
			obj1: testType{
				ID:     aUUID0,
				MyName: "foo",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "bar",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			expected: true,
			indexes:  []string{},
		},
		{
			name: "different",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
			},
			expected: false,
			indexes:  []string{},
		},
		{
			name: "same additional index",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   42,
			},
			expected: true,
			indexes:  []string{"int1"},
		},
		{
			name: "diff additional index",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   420,
			},
			expected: false,
			indexes:  []string{"int1"},
		},
		{
			name: "same additional indexes ",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
				Int2:   25,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   42,
				Int2:   25,
			},
			expected: true,
			indexes:  []string{"int1", "int2"},
		},
		{
			name: "diff additional indexes ",
			obj1: testType{
				ID:     aUUID0,
				MyName: "name1",
				Comp1:  "foo",
				Comp2:  "bar1",
				Int1:   42,
				Int2:   50,
			},
			obj2: testType{
				ID:     aUUID1,
				MyName: "name2",
				Comp1:  "foo",
				Comp2:  "bar2",
				Int1:   42,
				Int2:   25,
			},
			expected: false,
			indexes:  []string{"int1", "int2"},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("Equal %s", test.name), func(t *testing.T) {
			eq, err := api.Equal("TestTable", &test.obj1, &test.obj2, test.indexes...)
			if err != nil {
				t.Error(err)
			}
			if eq != test.expected {
				t.Errorf("Error comparing %+v, expected %t but got %t", test, test.expected, eq)
			}
		})
	}
}
