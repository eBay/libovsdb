package libovsdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type marshalSetTestTuple struct {
	objInput           interface{}
	jsonExpectedOutput string
}

type marshalMapsTestTuple struct {
	objInput           map[string]string
	jsonExpectedOutput string
}

var validUUIDStr0 = `00000000-0000-0000-0000-000000000000`
var validUUIDStr1 = `11111111-1111-1111-1111-111111111111`
var validUUID0 = UUID{GoUUID: validUUIDStr0}
var validUUID1 = UUID{GoUUID: validUUIDStr1}

var setTestList = []marshalSetTestTuple{
	{
		objInput:           []string{},
		jsonExpectedOutput: `["set",[]]`,
	},
	{
		objInput:           `aa`,
		jsonExpectedOutput: `"aa"`,
	},
	{
		objInput:           false,
		jsonExpectedOutput: `false`,
	},
	{
		objInput:           10,
		jsonExpectedOutput: `10`,
	},
	{
		objInput:           10.2,
		jsonExpectedOutput: `10.2`,
	},
	{
		objInput:           []string{`aa`},
		jsonExpectedOutput: `"aa"`,
	},
	{
		objInput:           [1]string{`aa`},
		jsonExpectedOutput: `"aa"`,
	},
	{
		objInput:           []string{`aa`, `bb`},
		jsonExpectedOutput: `["set",["aa","bb"]]`,
	},
	{
		objInput:           [2]string{`aa`, `bb`},
		jsonExpectedOutput: `["set",["aa","bb"]]`,
	},
	{
		objInput:           []int{10, 15},
		jsonExpectedOutput: `["set",[10,15]]`,
	},
	{
		objInput:           []uint16{10, 15},
		jsonExpectedOutput: `["set",[10,15]]`,
	},
	{
		objInput:           []float32{10.2, 15.4},
		jsonExpectedOutput: `["set",[10.2,15.4]]`,
	},
	{
		objInput:           []float64{10.2, 15.4},
		jsonExpectedOutput: `["set",[10.2,15.4]]`,
	},
	{
		objInput:           []UUID{},
		jsonExpectedOutput: `["set",[]]`,
	},
	{
		objInput:           UUID{GoUUID: `aa`},
		jsonExpectedOutput: `["named-uuid","aa"]`,
	},
	{
		objInput:           []UUID{{GoUUID: `aa`}},
		jsonExpectedOutput: `["named-uuid","aa"]`,
	},
	{
		objInput:           []UUID{{GoUUID: `aa`}, {GoUUID: `bb`}},
		jsonExpectedOutput: `["set",[["named-uuid","aa"],["named-uuid","bb"]]]`,
	},
	{
		objInput:           validUUID0,
		jsonExpectedOutput: fmt.Sprintf(`["uuid","%v"]`, validUUIDStr0),
	},
	{
		objInput:           []UUID{validUUID0},
		jsonExpectedOutput: fmt.Sprintf(`["uuid","%v"]`, validUUIDStr0),
	},
	{
		objInput:           []UUID{validUUID0, validUUID1},
		jsonExpectedOutput: fmt.Sprintf(`["set",[["uuid","%v"],["uuid","%v"]]]`, validUUIDStr0, validUUIDStr1),
	},
}

var mapTestList = []marshalMapsTestTuple{
	{
		objInput:           map[string]string{},
		jsonExpectedOutput: `["map",[]]`,
	},

	{
		objInput:           map[string]string{`v0`: `k0`},
		jsonExpectedOutput: `["map",[["v0","k0"]]]`,
	},

	{
		objInput:           map[string]string{`v0`: `k0`, `v1`: `k1`},
		jsonExpectedOutput: `["map",[["v0","k0"],["v1","k1"]]]`,
	},
}

// Json array is not order sensitive, but Golang set is, so we have to compare teh sets independently to the order of
// its elements
func setsAreEqual(t *testing.T, set1 *OvsSet, set2 *OvsSet) {
	res1 := map[interface{}]bool{}
	for _, elem := range set1.GoSet {
		switch elem.(type) {
		case UUID:
			uuid := elem.(UUID)
			res1[uuid.GoUUID] = true
		default:
			s := fmt.Sprintf("%v", elem)
			res1[s] = true
		}
	}

	res2 := map[interface{}]bool{}
	for _, elem := range set2.GoSet {
		switch elem.(type) {
		case UUID:
			uuid := elem.(UUID)
			res2[uuid.GoUUID] = true
		default:
			s := fmt.Sprintf("%v", elem)
			res2[s] = true
		}
	}
	assert.Equal(t, res1, res2, "they should be equal\n")
}

func TestMarshalSet(t *testing.T) {

	for _, e := range setTestList {
		set, err := NewOvsSet(e.objInput)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(set)
		assert.Nil(t, err)
		assert.JSONEqf(t, e.jsonExpectedOutput, string(jsonStr), "they should be equal\n")
	}

}

func TestMarshalMap(t *testing.T) {

	for _, e := range mapTestList {
		m, err := NewOvsMap(e.objInput)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(m)
		assert.Nil(t, err)
		assert.JSONEqf(t, e.jsonExpectedOutput, string(jsonStr), "they should be equal\n")
	}

}

func TestUnmarshalSet(t *testing.T) {

	for _, e := range setTestList {
		set, err := NewOvsSet(e.objInput)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(set)
		assert.Nil(t, err)
		var res OvsSet
		err = json.Unmarshal(jsonStr, &res)
		assert.Nil(t, err)
		setsAreEqual(t, set, &res)
	}

}

func TestUnmarshalMap(t *testing.T) {

	for _, e := range mapTestList {
		m, err := NewOvsMap(e.objInput)
		assert.Nil(t, err)
		jsonStr, err := json.Marshal(m)
		assert.Nil(t, err)
		var res OvsMap
		err = json.Unmarshal(jsonStr, &res)
		assert.Nil(t, err)
		assert.Equal(t, *m, res, "they should be equal\n")
	}
}
