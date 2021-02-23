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
func (oa ORMAPI) NewRow(tableName string, data interface{}) (map[string]interface{}, error) {
	table, ok := oa.schema.Tables[tableName]
	if !ok {
		return nil, NewErrNoTable(tableName)
	}
	objPtrVal := reflect.ValueOf(data)
	if objPtrVal.Type().Kind() != reflect.Ptr {
		return nil, NewErrWrongType("ORMAPI.NewRow", "pointer to a struct", data)
	}
	objVal := reflect.Indirect(objPtrVal)
	fields, err := oa.getORMFields(&table, objVal.Type())
	if err != nil {
		return nil, err
	}
	ovsRow := make(map[string]interface{}, len(table.Columns))
	for name, column := range table.Columns {
		fieldName, ok := fields[name]
		if !ok {
			// If provided struct does not have a field to hold this value, skip it
			continue
		}

		nativeElem := objVal.FieldByName(fieldName)
		ovsElem, err := NativeToOvs(column, nativeElem.Interface())
		if err != nil {
			return nil, fmt.Errorf("Table %s, Column %s: Failed to generate OvS element. %s", tableName, name, err.Error())
		}
		ovsRow[name] = ovsElem
	}
	return ovsRow, nil

}

// ormField contains the field information of a ORM
// It's a map [string] string. Where the key is the column name and the value is the name of the
// field in which the value of such column shall be stored / read from
type ormFields map[string]string

//
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

// NewCondition returns a valid condition to be used inside a Operation
// Use the native API
func (oa ORMAPI) NewCondition(tableName, columnName, function string, value interface{}) ([]interface{}, error) {
	return NativeAPI{schema: oa.schema}.NewCondition(tableName, columnName, function, value)
}

// NewMutation returns a valid mutation to be used inside a Operation
// It accepts native golang types (sets and maps)
func (oa ORMAPI) NewMutation(tableName, columnName, mutator string, value interface{}) ([]interface{}, error) {
	return NativeAPI{schema: oa.schema}.NewMutation(tableName, columnName, mutator, value)
}
