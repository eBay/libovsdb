package libovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowCache_Row(t *testing.T) {
	type fields struct {
		cache map[string]Row
	}
	type args struct {
		uuid string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Row
	}{
		{
			"returns a row that exists",
			fields{cache: map[string]Row{"test": {}}},
			args{uuid: "test"},
			&Row{},
		},
		{
			"returns a nil for a row that does not exist",
			fields{cache: map[string]Row{"test": {}}},
			args{uuid: "foo"},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowCache{
				cache: tt.fields.cache,
			}
			got := r.Row(tt.args.uuid)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRowCache_Rows(t *testing.T) {
	type fields struct {
		cache map[string]Row
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			"returns a rows that exist",
			fields{cache: map[string]Row{"test1": {}, "test2": {}, "test3": {}}},
			[]string{"test1", "test2", "test3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowCache{
				cache: tt.fields.cache,
			}
			got := r.Rows()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestEventHandlerFuncs_OnAdd(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row Row)
		UpdateFunc func(table string, old Row, new Row)
		DeleteFunc func(table string, row Row)
	}
	type args struct {
		table string
		row   Row
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", Row{}},
		},
		{
			"calls onadd function",
			fields{func(string, Row) {
				calls++
			}, nil, nil},
			args{"testTable", Row{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnAdd(tt.args.table, tt.args.row)
			if e.AddFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestEventHandlerFuncs_OnUpdate(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row Row)
		UpdateFunc func(table string, old Row, new Row)
		DeleteFunc func(table string, row Row)
	}
	type args struct {
		table string
		old   Row
		new   Row
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", Row{}, Row{}},
		},
		{
			"calls onupdate function",
			fields{nil, func(string, Row, Row) {
				calls++
			}, nil},
			args{"testTable", Row{}, Row{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnUpdate(tt.args.table, tt.args.old, tt.args.new)
			if e.UpdateFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestEventHandlerFuncs_OnDelete(t *testing.T) {
	calls := 0
	type fields struct {
		AddFunc    func(table string, row Row)
		UpdateFunc func(table string, old Row, new Row)
		DeleteFunc func(table string, row Row)
	}
	type args struct {
		table string
		row   Row
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"doesn't call nil function",
			fields{nil, nil, nil},
			args{"testTable", Row{}},
		},
		{
			"calls ondelete function",
			fields{nil, nil, func(string, Row) {
				calls++
			}},
			args{"testTable", Row{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventHandlerFuncs{
				AddFunc:    tt.fields.AddFunc,
				UpdateFunc: tt.fields.UpdateFunc,
				DeleteFunc: tt.fields.DeleteFunc,
			}
			e.OnDelete(tt.args.table, tt.args.row)
			if e.DeleteFunc != nil {
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestTableCache_Table(t *testing.T) {
	type fields struct {
		cache map[string]*RowCache
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RowCache
	}{
		{
			"returns nil for an empty table",
			fields{
				cache: map[string]*RowCache{"bar": newRowCache()},
			},
			args{
				"foo",
			},
			nil,
		},
		{
			"returns nil for an empty table",
			fields{
				cache: map[string]*RowCache{"bar": newRowCache()},
			},
			args{
				"bar",
			},
			newRowCache(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TableCache{
				cache: tt.fields.cache,
			}
			got := tr.Table(tt.args.name)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTableCache_Tables(t *testing.T) {
	type fields struct {
		cache map[string]*RowCache
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			"returns a table that exists",
			fields{cache: map[string]*RowCache{"test1": newRowCache(), "test2": newRowCache(), "test3": newRowCache()}},
			[]string{"test1", "test2", "test3"},
		},
		{
			"returns an empty slice if no tables exist",
			fields{cache: map[string]*RowCache{}},
			[]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &TableCache{
				cache: tt.fields.cache,
			}
			got := tr.Tables()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestTableCache_populate(t *testing.T) {
	t.Log("Create")
	tc := newTableCache()
	testRow := Row{Fields: map[string]interface{}{"_uuid": "test", "foo": "bar"}}
	updates := TableUpdates{
		Updates: map[string]TableUpdate{
			"Open_vSwitch": {
				Rows: map[string]RowUpdate{
					"test": {
						Old: Row{},
						New: testRow,
					},
				},
			},
		},
	}
	tc.populate(updates)

	got := tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, testRow, got)

	t.Log("Update")
	updatedRow := Row{Fields: map[string]interface{}{"_uuid": "test", "foo": "quux"}}
	updates = TableUpdates{
		Updates: map[string]TableUpdate{
			"Open_vSwitch": {
				Rows: map[string]RowUpdate{
					"test": {
						Old: testRow,
						New: updatedRow,
					},
				},
			},
		},
	}
	tc.populate(updates)

	got = tc.cache["Open_vSwitch"].cache["test"]
	assert.Equal(t, updatedRow, got)

	t.Log("Delete")
	updates = TableUpdates{
		Updates: map[string]TableUpdate{
			"Open_vSwitch": {
				Rows: map[string]RowUpdate{
					"test": {
						Old: updatedRow,
						New: Row{},
					},
				},
			},
		},
	}

	tc.populate(updates)

	_, ok := tc.cache["Open_vSwitch"].cache["test"]
	assert.False(t, ok)
}

func TestTableCache_AddEventHandler(t *testing.T) {
	tc := newTableCache()
	tc.AddEventHandler(&EventHandlerFuncs{nil, nil, nil})
	assert.Equal(t, 1, len(tc.handlers))
}
