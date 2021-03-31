package libovsdb

import "fmt"

var (
	aString  = "foo"
	aEnum    = "enum1"
	aEnumSet = []string{"enum1", "enum2", "enum3"}
	aSet     = []string{"a", "set", "of", "strings"}
	aUUID0   = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1   = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2   = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3   = "2f77b348-9768-4866-b761-89d5177ecda3"

	aUUIDSet = []string{
		aUUID0,
		aUUID1,
		aUUID2,
		aUUID3,
	}

	aIntSet = []int{
		0,
		1,
		2,
		3,
	}
	aFloat = 42.00

	aInt = 42

	aFloatSet = []float64{
		3.14,
		2.71,
		42.0,
	}

	aMap = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	aEmptySet = []string{}
)

var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "columns": {
        "aString": {
          "type": "string"
        },
        "aSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        },
        "aSingleSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "max": 1
          }
        },
        "aUUIDSet": {
          "type": {
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
          }
        },
        "aUUID": {
          "type": {
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 1,
            "max": 1
          }
        },
        "aIntSet": {
          "type": {
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        },
        "aFloat": {
          "type": {
            "key": {
              "type": "real"
            }
          }
        },
        "aFloatSet": {
          "type": {
            "key": {
              "type": "real"
            },
            "min": 0,
            "max": 10
          }
        },
        "aEmptySet": {
          "type": {
            "key": {
              "type": "string"
            },
            "min": 0,
            "max": "unlimited"
          }
        },
        "aEnum": {
          "type": {
            "key": {
              "enum": [
                "set",
                [
                  "enum1",
                  "enum2",
                  "enum3"
                ]
              ],
              "type": "string"
            }
          }
        },
        "aMap": {
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

// When going Native -> OvS:
//	map -> *OvsMap
//	slice -> *OvsSet
// However, when going OvS -> Native
//	OvsMap -> map
//	OvsSet -> slice
// Perform indirection of ovs fields to be compared
// with the ones that wre used initially
func expectedOvs(in interface{}) interface{} {
	switch in.(type) {
	case *OvsSet:
		return *(in.(*OvsSet))
	case *OvsMap:
		return *(in.(*OvsMap))
	default:
		return in
	}
}

func getNativeTestMap() map[string]interface{} {
	return map[string]interface{}{
		"aString":    aString,
		"aSet":       aSet,
		"ASingleSet": []string{aString},
		"aUUIDSet":   aUUIDSet,
		"aMap":       aMap,
		"aFloat":     aFloat,
		"aFloatSet":  aFloatSet,
		"aUUID":      aUUID0,
		"aIntSet":    aIntSet,
	}
}

func getOvsTestRow() Row {
	ovsRow := Row{Fields: make(map[string]interface{})}
	ovsRow.Fields["aString"] = aString
	s, _ := NewOvsSet(aSet)
	ovsRow.Fields["aSet"] = *s

	// Set's can hold the value if they have len == 1
	ovsRow.Fields["aSingleSet"] = aString

	us := make([]UUID, 0)
	for _, u := range aUUIDSet {
		us = append(us, UUID{GoUUID: u})
	}
	ovsUs, _ := NewOvsSet(us)
	ovsRow.Fields["aUUIDSet"] = *ovsUs

	ovsRow.Fields["aUUID"] = UUID{GoUUID: aUUID0}

	is, e := NewOvsSet(aIntSet)
	if e != nil {
		fmt.Printf("%s", e.Error())
	}
	ovsRow.Fields["aIntSet"] = *is

	ovsRow.Fields["aFloat"] = aFloat

	fs, e := NewOvsSet(aFloatSet)
	ovsRow.Fields["aFloatSet"] = *fs

	es, e := NewOvsSet([]string{})
	ovsRow.Fields["aEmptySet"] = *es

	ovsRow.Fields["aEnum"] = aEnum

	m, _ := NewOvsMap(aMap)
	ovsRow.Fields["aMap"] = *m

	return ovsRow
}
