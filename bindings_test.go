package libovsdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

type Transformation map[string]interface{}

func (t Transformation) String() string {
	return fmt.Sprintf("Transformation: \n\t schema: %s\n\t native: %v\n\t native2ovs: %v\n\t ovs: %v\n\t ovs2native: %v\n\t",
		string(t["schema"].([]byte)), t["native"], t["native2ovs"], t["ovs"], t["ovs2native"])

}

func getErrTransMaps() []map[string]interface{} {
	var transMap []map[string]interface{}
	transMap = append(transMap, map[string]interface{}{
		"name":   "Wrong Atomic Type",
		"schema": []byte(`{"type":"string"}`),
		"native": 42,
		"ovs":    42,
	})
	transMap = append(transMap, map[string]interface{}{
		"name":   "Wrong Atomic Numeric Type: Int",
		"schema": []byte(`{"type":"integer"}`),
		"native": 42.0,
		"ovs":    42.0,
	})
	transMap = append(transMap, map[string]interface{}{
		"name":   "Wrong Atomic Numeric Type: Float",
		"schema": []byte(`{"type":"real"}`),
		"native": 42,
		"ovs":    42,
	})
	as, _ := NewOvsSet([]string{"foo"})
	transMap = append(transMap, map[string]interface{}{
		"name":   "Set instead of Atomic Type",
		"schema": []byte(`{"type":"string"}`),
		"native": []string{"foo"},
		"ovs":    *as,
	})
	transMap = append(transMap, map[string]interface{}{
		"name": "Wrong Set Type",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native": []int{1, 2},
		"ovs":    []int{1, 2},
	})

	s, _ := NewOvsMap(map[string]string{"foo": "bar"})
	transMap = append(transMap, map[string]interface{}{
		"name": "Wrong Map instead of Set",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native": map[string]string{"foo": "bar"},
		"ovs":    *s,
	})

	m, _ := NewOvsMap(map[int]string{1: "one", 2: "two"})
	transMap = append(transMap, map[string]interface{}{
		"name": "Wrong Map key type",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}`),
		"native": map[int]string{1: "one", 2: "two"},
		"ovs":    *m,
	})
	return transMap
}
func getTransMaps() []map[string]interface{} {
	var transMap []map[string]interface{}
	// String
	transMap = append(transMap, map[string]interface{}{
		"name":       "String",
		"schema":     []byte(`{"type":"string"}`),
		"native":     aString,
		"native2ovs": aString,
		"ovs":        aString,
		"ovs2native": aString,
	})

	// Float
	transMap = append(transMap, map[string]interface{}{
		"name":       "Float",
		"schema":     []byte(`{"type":"real"}`),
		"native":     aFloat,
		"native2ovs": aFloat,
		"ovs":        aFloat,
		"ovs2native": aFloat,
	})

	// string set
	s, _ := NewOvsSet(aSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "String Set",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native":     aSet,
		"native2ovs": s,
		"ovs":        *s,
		"ovs2native": aSet,
	})

	// string with exactly one element can also be represented
	// as the element itself. On ovs2native, we keep the slice representation
	s1, _ := NewOvsSet([]string{aString})
	transMap = append(transMap, map[string]interface{}{
		"name": "String Set with exactly one field",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native":     []string{aString},
		"native2ovs": s1,
		"ovs":        aString,
		"ovs2native": []string{aString},
	})

	// UUID set
	us := make([]UUID, 0)
	for _, u := range aUUIDSet {
		us = append(us, UUID{GoUUID: u})
	}
	uss, _ := NewOvsSet(us)
	transMap = append(transMap, map[string]interface{}{
		"name": "UUID Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
         }
	}`),
		"native":     aUUIDSet,
		"native2ovs": uss,
		"ovs":        *uss,
		"ovs2native": aUUIDSet,
	})

	// UUID set with exactly one element.
	us1 := []UUID{{GoUUID: aUUID0}}
	uss1, _ := NewOvsSet(us1)
	transMap = append(transMap, map[string]interface{}{
		"name": "UUID Set with exactly one field",
		"schema": []byte(`{
	"type":{
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
         }
	}`),
		"native":     []string{aUUID0},
		"native2ovs": uss1,
		"ovs":        UUID{GoUUID: aUUID0},
		"ovs2native": []string{aUUID0},
	})

	// A integer set
	is, _ := NewOvsSet(aIntSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Integer Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aIntSet,
		"native2ovs": is,
		"ovs":        *is,
		"ovs2native": aIntSet,
	})

	// A float set
	fs, _ := NewOvsSet(aFloatSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Float Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "real"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aFloatSet,
		"native2ovs": fs,
		"ovs":        *fs,
		"ovs2native": aFloatSet,
	})

	// A empty string set
	es, _ := NewOvsSet(aEmptySet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Empty String Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "string"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aEmptySet,
		"native2ovs": es,
		"ovs":        *es,
		"ovs2native": aEmptySet,
	})

	// Enum
	transMap = append(transMap, map[string]interface{}{
		"name": "Enum (string)",
		"schema": []byte(`{
	"type":{
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
	}`),
		"native":     aEnum,
		"native2ovs": aEnum,
		"ovs":        aEnum,
		"ovs2native": aEnum,
	})

	// Enum set
	ens, _ := NewOvsSet(aEnumSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Enum Set (string)",
		"schema": []byte(`{
	"type":{
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
            },
	    "max": "unlimited",
	    "min": 0
          }
	}`),
		"native":     aEnumSet,
		"native2ovs": ens,
		"ovs":        *ens,
		"ovs2native": aEnumSet,
	})

	// A Map
	m, _ := NewOvsMap(aMap)
	transMap = append(transMap, map[string]interface{}{
		"name": "Map (string->string)",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}`),
		"native":     aMap,
		"native2ovs": m,
		"ovs":        *m,
		"ovs2native": aMap,
	})
	return transMap
}

func TestOvsToNative(t *testing.T) {
	transMaps := getTransMaps()
	for _, trans := range transMaps {
		t.Run(fmt.Sprintf("Ovs To Native: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := OvsToNative(&column, trans["ovs"])
			if err != nil {
				t.Errorf("Failed to convert %s: %s", trans, err)
				t.Logf("Testing %v", string(trans["schema"].([]byte)))
			}

			if !reflect.DeepEqual(res, trans["ovs2native"]) {
				t.Errorf("Fail to convert ovs2native. OVS: %v(%s). Expected %v(%s). Got %v (%s)",
					trans["ovs"], reflect.TypeOf(trans["ovs"]),
					trans["ovs2native"], reflect.TypeOf(trans["ovs2native"]),
					res, reflect.TypeOf(res))
			}
		})
	}
}

func TestNativeToOvs(t *testing.T) {
	transMaps := getTransMaps()
	for _, trans := range transMaps {
		t.Run(fmt.Sprintf("Native To Ovs: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := NativeToOvs(&column, trans["native"])
			if err != nil {
				t.Errorf("Failed to convert %s: %s", trans, err)
				t.Logf("Testing %v", string(trans["schema"].([]byte)))
			}

			if !reflect.DeepEqual(res, trans["native2ovs"]) {
				t.Errorf("Fail to convert native2ovs. Native: %v(%s). Expected %v(%s). Got %v (%s)",
					trans["native"], reflect.TypeOf(trans["native"]),
					trans["native2ovs"], reflect.TypeOf(trans["native2ovs"]),
					res, reflect.TypeOf(res))
			}
		})
	}
}

func TestOvsToNativeErr(t *testing.T) {
	transMaps := getErrTransMaps()
	for _, trans := range transMaps {
		t.Run(fmt.Sprintf("Ovs To Native Error: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := OvsToNative(&column, trans["ovs"])
			if err == nil {
				t.Errorf("Convertion %s should have failed, instead it has returned %v (%s)", trans, res, reflect.TypeOf(res))
				t.Logf("Conversion schema %v", string(trans["schema"].([]byte)))
			}
		})
	}
}

func TestNativeToOvsErr(t *testing.T) {
	transMaps := getErrTransMaps()
	for _, trans := range transMaps {
		t.Run(fmt.Sprintf("Native To Ovs Error: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := NativeToOvs(&column, trans["native"])
			if err == nil {
				t.Errorf("Convertion %s should have failed, instead it has returned %v (%s)", trans, res, reflect.TypeOf(res))
				t.Logf("Conversion schema %v", string(trans["schema"].([]byte)))
			}
		})
	}
}
