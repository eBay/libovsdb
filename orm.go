package libovsdb

import (
	"fmt"
	"reflect"
)

// ORMAPI is an API that offers functions to interact with libovsdb through
// user-provided native structs. The way to specify what field of the struct goes
// to what column in the database id through field a field tag.
// The tag used is "ovs" and has the following structure
// 'ovn:"${COLUMN_NAME}"'
//	where COLUMN_NAME is the name of the column and must match the schema
//
//Example:
//  type MyObj struct {
//  	Name string `ovs:"name"`
//  }
type ORMAPI struct {
	schema *DatabaseSchema
}

// ErrORM describes an error in an ORM type
type ErrORM struct {
	objType   string
	field     string
	fieldType string
	fieldTag  string
	reason    string
}

func (e *ErrORM) Error() string {
	return fmt.Sprintf("ORM Error. Object type %s contains field %s (%s) ovs tag %s: %s",
		e.objType, e.field, e.fieldType, e.fieldTag, e.reason)
}

// ormFields contains the field information of a ORM
// It's a map [string] string. Where the key is the column name and the value is the name of the
// field in which the value of such column shall be stored / read from
type ormFields = map[string]string

// NewORMAPI returns a new ORM API
func NewORMAPI(schema *DatabaseSchema) *ORMAPI {
	return &ORMAPI{
		schema: schema,
	}
}

// GetRowData transforms a Row to a struct based on its tags
// The result object must be given as pointer to an object with the right tags
func (oa ORMAPI) GetRowData(tableName string, row *Row, result interface{}) error {
	if row == nil {
		return nil
	}
	return oa.GetData(tableName, row.Fields, result)
}

// GetData transforms a map[string]interface{} containing OvS types (e.g: a ResultRow
// has this format) to ORM struct
// The result object must be given as pointer to an object with the right tags
func (oa ORMAPI) GetData(tableName string, ovsData map[string]interface{}, result interface{}) error {
	table, ok := oa.schema.Tables[tableName]
	if !ok {
		return NewErrNoTable(tableName)
	}

	objPtrVal := reflect.ValueOf(result)
	if objPtrVal.Type().Kind() != reflect.Ptr {
		return NewErrWrongType("ORMAPI.GetData", "pointer to a struct", result)
	}

	objVal := reflect.Indirect(objPtrVal)
	fields, err := oa.getORMFields(&table, objVal.Type())
	if err != nil {
		return err
	}
	for name, column := range table.Columns {
		fieldName, ok := fields[name]
		if !ok {
			// If provided struct does not have a field to hold this value, skip it
			continue
		}

		ovsElem, ok := ovsData[name]
		if !ok {
			// Ignore missing columns
			continue
		}

		nativeElem, err := OvsToNative(column, ovsElem)
		if err != nil {
			return fmt.Errorf("Table %s, Column %s: Failed to extract native element: %s",
				tableName, name, err.Error())
		}

		nativeElemValue := reflect.ValueOf(nativeElem)
		destFieldValue := objVal.FieldByName(fieldName)
		if !destFieldValue.Type().AssignableTo(nativeElemValue.Type()) {
			return fmt.Errorf("Table %s, Column %s: Native value %v (%s) is not assignable to field %s (%s)",
				tableName, name, nativeElem, nativeElemValue.Type(), fieldName, destFieldValue.Type())
		}
		destFieldValue.Set(nativeElemValue)
	}
	return nil
}

// NewRow transforms an ORM struct to a map[string] interface{} that can be used as libovsdb.Row
// By default all non-default values in the ORM struct will be used.
// If columns are explicitly provided, the resulting row will only contain such columns (regardless of the
// content of the associated field in the ORM struct).
func (oa ORMAPI) NewRow(tableName string, data interface{}, columns ...string) (map[string]interface{}, error) {
	table, ok := oa.schema.Tables[tableName]
	if !ok {
		return nil, NewErrNoTable(tableName)
	}
	objPtrVal := reflect.ValueOf(data)
	if objPtrVal.Type().Kind() != reflect.Ptr {
		return nil, NewErrWrongType("ORMAPI.NewRow", "pointer to a struct", data)
	}
	objVal := reflect.Indirect(objPtrVal)
	ormFields, err := oa.getORMFields(&table, objVal.Type())
	if err != nil {
		return nil, err
	}

	ovsRow := make(map[string]interface{}, len(table.Columns))
	for name, column := range table.Columns {
		fieldName, ok := ormFields[name]
		if !ok {
			// If provided struct does not have a field to hold this value, skip it
			continue
		}

		if len(columns) > 0 {
			found := false
			for _, col := range columns {
				if col == name {
					found = true
					break
				}
			}
			if found == false {
				continue
			}
		}

		nativeElem := objVal.FieldByName(fieldName)

		// Omit fields with default or nil value except if the column was explicitly provided
		if len(columns) == 0 && IsDefaultValue(column, nativeElem.Interface()) {
			continue
		}
		ovsElem, err := NativeToOvs(column, nativeElem.Interface())
		if err != nil {
			return nil, fmt.Errorf("Table %s, Column %s: Failed to generate OvS element. %s", tableName, name, err.Error())
		}
		ovsRow[name] = ovsElem
	}
	return ovsRow, nil
}

// NewCondition returns a valid condition to be used inside a Operation
// Use the native API
//func (oa ORMAPI) NewCondition(tableName, columnName, function string, value interface{}) ([]interface{}, error) {
//	return NativeAPI{schema: oa.schema}.NewCondition(tableName, columnName, function, value)
//}

// NewCondition returns a list of conditions that match a given object
// A list of valid columns that shall be used as a index can be provided.
// If none are provided, we will try to use object's field that matches the '_uuid' ovs tag
// If it does not exist or is null (""), then we will traverse all of the table indexes and
// use the first index (list of simultaneously unique columnns) for witch the provided ORM
// object has valid data. The order in which they are traversed matches the order defined
// in the schema.
// By `valid data` we mean non-default data.
func (oa ORMAPI) NewCondition(tableName string, data interface{}, index ...string) ([]interface{}, error) {
	var conditions []interface{}
	var condIndex [][]string

	table, ok := oa.schema.Tables[tableName]
	if !ok {
		return nil, NewErrNoTable(tableName)
	}
	objPtrVal := reflect.ValueOf(data)
	if objPtrVal.Type().Kind() != reflect.Ptr {
		return nil, NewErrWrongType("ORMAPI.NewCondition", "pointer to a struct", data)
	}
	objVal := reflect.Indirect(objPtrVal)
	fields, err := oa.getORMFields(&table, objVal.Type())
	if err != nil {
		return nil, err
	}

	// If index is provided, use it. If not, inspect the schema (and include _uuid)
	if len(index) > 0 {
		condIndex = append(condIndex, index)
	} else {
		var err error
		condIndex, err = oa.getValidORMIndexes(&table, fields, objVal)
		if err != nil {
			return nil, err
		}
	}

	if len(condIndex) == 0 {
		return nil, fmt.Errorf("Failed to find a valid index")
	}

	// Pick the first valid index
	for _, col := range condIndex[0] {
		fieldName, _ := fields[col]
		fieldVal := objVal.FieldByName(fieldName)

		column, err := oa.schema.GetColumn(tableName, col)
		if err != nil {
			return nil, err
		}
		ovsVal, err := NativeToOvs(column, fieldVal.Interface())
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, []interface{}{col, "==", ovsVal})
	}
	return conditions, nil
}

// NewMutation returns a valid mutation to be used inside a Operation
// It accepts native golang types (sets and maps)
// TODO: check mutator validity
func (oa ORMAPI) NewMutation(tableName, columnName, mutator string, value interface{}) ([]interface{}, error) {
	return NativeAPI{schema: oa.schema}.NewMutation(tableName, columnName, mutator, value)
}

// Equal returns whether both models are equal from the DB point of view
// Two objectes are considered equal if any of the following conditions is true
// They have a field tagged with column name '_uuid' and their values match
// For any of the indexes defined in the Table Schema, the values all of its columns are simultaneously equal
// (as per RFC7047)
// The values of all of the optional indexes passed as variadic parameter to this function are equal.
func (oa ORMAPI) Equal(tableName string, lhs, rhs interface{}, indexes ...string) (bool, error) {
	match := false

	table, ok := oa.schema.Tables[tableName]
	if !ok {
		return false, NewErrNoTable(tableName)
	}
	// Obtain indexes from lhs
	lhsValPtr := reflect.ValueOf(lhs)
	if lhsValPtr.Type().Kind() != reflect.Ptr {
		return false, NewErrWrongType("ORMAPI.Equal", "pointer to a struct", lhs)
	}
	lhsVal := reflect.Indirect(lhsValPtr)

	lfields, err := oa.getORMFields(&table, lhsVal.Type())
	if err != nil {
		return false, err
	}
	lhsIndexes, err := oa.getValidORMIndexes(&table, lfields, lhsVal)
	if err != nil {
		return false, err
	}
	lhsIndexes = append(lhsIndexes, indexes)

	// Obtain indexes from rhs
	rhsValPtr := reflect.ValueOf(rhs)
	if rhsValPtr.Type().Kind() != reflect.Ptr {
		return false, NewErrWrongType("ORMAPI.Equal", "pointer to a struct", rhs)
	}
	rhsVal := reflect.Indirect(rhsValPtr)

	rfields, err := oa.getORMFields(&table, rhsVal.Type())
	if err != nil {
		return false, err
	}
	rhsIndexes, err := oa.getValidORMIndexes(&table, rfields, rhsVal)
	if err != nil {
		return false, err
	}
	rhsIndexes = append(rhsIndexes, indexes)

	for _, lidx := range lhsIndexes {
		for _, ridx := range rhsIndexes {
			if reflect.DeepEqual(ridx, lidx) {
				// All columns in an index must be simultaneously equal
				for _, col := range lidx {
					lfieldName, ok := lfields[col]
					if !ok {
						break
					}
					lval := reflect.Indirect(reflect.ValueOf(lhs)).FieldByName(lfieldName)
					rfieldName, ok := rfields[col]
					if !ok {
						break
					}
					rval := reflect.Indirect(reflect.ValueOf(rhs)).FieldByName(rfieldName)
					if reflect.DeepEqual(lval.Interface(), rval.Interface()) {
						match = true
					} else {
						match = false
						break
					}
				}
				if match == true {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (oa ORMAPI) getORMFields(table *TableSchema, objType reflect.Type) (ormFields, error) {
	fields := make(ormFields, objType.NumField())
	for i := 0; i < objType.NumField(); i++ {
		field := objType.Field(i)
		colName := field.Tag.Get("ovs")
		if colName == "" {
			// Untagged fields are ignored
			continue
		}
		column, err := table.GetColumn(colName)
		if err != nil {
			return nil, &ErrORM{
				objType:   objType.String(),
				field:     field.Name,
				fieldType: field.Type.String(),
				fieldTag:  colName,
				reason:    "Column does not exist in schema",
			}
		}

		// Perform schema-based type checking
		expType := nativeType(column)
		if expType != field.Type {
			return nil, &ErrORM{
				objType:   objType.String(),
				field:     field.Name,
				fieldType: field.Type.String(),
				fieldTag:  colName,
				reason:    fmt.Sprintf("Wrong type, column expects %s", expType),
			}
		}
		fields[colName] = field.Name
	}
	return fields, nil
}

// getValidORMIndexes inspects the object and returns the a list of indexes (set of columns) for witch
// the object has non-default values
func (oa ORMAPI) getValidORMIndexes(table *TableSchema, fields ormFields, objVal reflect.Value) ([][]string, error) {
	var validIndexes [][]string
	var possibleIndexes [][]string

	possibleIndexes = append(possibleIndexes, []string{"_uuid"})
	for _, columnSet := range table.Indexes {
		possibleIndexes = append(possibleIndexes, columnSet)
	}

	// Iterate through indexes and validate them
OUTER:
	for _, idx := range possibleIndexes {
		for _, col := range idx {
			fieldName, ok := fields[col]
			if !ok {
				continue OUTER
			}
			columnSchema, err := table.GetColumn(col)
			if err != nil {
				continue OUTER
			}
			fieldVal := objVal.FieldByName(fieldName)
			if !fieldVal.IsValid() || IsDefaultValue(columnSchema, fieldVal.Interface()) {
				continue OUTER
			}
		}
		validIndexes = append(validIndexes, idx)
	}
	return validIndexes, nil
}
