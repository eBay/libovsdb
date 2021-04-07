package libovsdb

import (
	"reflect"
	"sync"
)

// RowCache is a collections of Rows hashed by UUID
type RowCache struct {
	cache map[string]Row
	mutex sync.Mutex
}

// Row returns one row the from the cache by uuid
func (r *RowCache) Row(uuid string) *Row {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if row, ok := r.cache[uuid]; ok {
		return &row
	}
	return nil
}

// Rows returns a list of row UUIDs as strings
func (r *RowCache) Rows() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var result []string
	for k := range r.cache {
		result = append(result, k)
	}
	return result
}

func newRowCache() *RowCache {
	return &RowCache{
		cache: make(map[string]Row),
		mutex: sync.Mutex{},
	}
}

// EventHandler can handle events that happen to cache rowects
type EventHandler interface {
	OnAdd(table string, row Row)
	OnUpdate(table string, old Row, new Row)
	OnDelete(table string, row Row)
}

// EventHandlerFuncs is a wrapper for the EventHandler interface
// It allows a caller to only implement the functions they need
type EventHandlerFuncs struct {
	AddFunc    func(table string, row Row)
	UpdateFunc func(table string, old Row, new Row)
	DeleteFunc func(table string, row Row)
}

// OnAdd calls AddFunc if it is not nil
func (e *EventHandlerFuncs) OnAdd(table string, row Row) {
	if e.AddFunc != nil {
		e.AddFunc(table, row)
	}
}

// OnUpdate calls UpdateFunc if it is not nil
func (e *EventHandlerFuncs) OnUpdate(table string, old, new Row) {
	if e.UpdateFunc != nil {
		e.UpdateFunc(table, old, new)
	}
}

// OnDelete calls DeleteFunc if it is not nil
func (e *EventHandlerFuncs) OnDelete(table string, row Row) {
	if e.DeleteFunc != nil {
		e.DeleteFunc(table, row)
	}
}

// TableCache is a collection of TableCache hashed by database name
type TableCache struct {
	cache         map[string]*RowCache
	cacheMutex    sync.Mutex
	handlers      []EventHandler
	handlersMutex sync.Mutex
}

func newTableCache() *TableCache {
	return &TableCache{
		cache:         make(map[string]*RowCache),
		cacheMutex:    sync.Mutex{},
		handlersMutex: sync.Mutex{},
	}
}

// Table returns the from the cache
func (t *TableCache) Table(name string) *RowCache {
	t.cacheMutex.Lock()
	defer t.cacheMutex.Unlock()
	if table, ok := t.cache[name]; ok {
		return table
	}
	return nil
}

// Tables returns a list of tables
func (t *TableCache) Tables() []string {
	t.cacheMutex.Lock()
	defer t.cacheMutex.Unlock()
	var result []string
	for k := range t.cache {
		result = append(result, k)
	}
	return result
}

// Update implements the update method of the NotificationHandler interface
// this populates the cache with new updates
func (t *TableCache) Update(context interface{}, tableUpdates TableUpdates) {
	if len(tableUpdates.Updates) == 0 {
		return
	}
	go t.populate(tableUpdates)
}

// Locked implements the locked method of the NotificationHandler interface
func (t *TableCache) Locked([]interface{}) {
}

// Stolen implements the stolen method of the NotificationHandler interface
func (t *TableCache) Stolen([]interface{}) {
}

// Echo implements the echo method of the NotificationHandler interface
func (t *TableCache) Echo([]interface{}) {
}

// Disconnected implements the disconnected method of the NotificationHandler interface
func (t *TableCache) Disconnected() {
}

func (t *TableCache) populate(tableUpdates TableUpdates) {
	t.cacheMutex.Lock()
	defer t.cacheMutex.Unlock()
	for table, updates := range tableUpdates.Updates {
		var tCache *RowCache
		var ok bool
		if tCache, ok = t.cache[table]; !ok {
			t.cache[table] = newRowCache()
			tCache = t.cache[table]
		}
		tCache.mutex.Lock()
		for uuid, row := range updates.Rows {
			if !reflect.DeepEqual(row.New, Row{}) {
				if existing, ok := tCache.cache[uuid]; ok {
					if !reflect.DeepEqual(row.New, existing) {
						tCache.cache[uuid] = row.New
						t.handlersMutex.Lock()
						for _, handler := range t.handlers {
							go handler.OnUpdate(table, row.Old, row.New)
						}
						t.handlersMutex.Unlock()
					}
					// no diff
					continue
				}
				tCache.cache[uuid] = row.New
				t.handlersMutex.Lock()
				for _, handler := range t.handlers {
					go handler.OnAdd(table, row.New)
				}
				t.handlersMutex.Unlock()
				continue
			} else {
				// delete from cache
				delete(tCache.cache, uuid)
				t.handlersMutex.Lock()
				for _, handler := range t.handlers {
					go handler.OnDelete(table, row.Old)
				}
				t.handlersMutex.Unlock()
				continue
			}
		}
		tCache.mutex.Unlock()
	}
}

// AddEventHandler registers the supplied EventHandler to recieve cache events
func (t *TableCache) AddEventHandler(handler EventHandler) {
	t.handlersMutex.Lock()
	defer t.handlersMutex.Unlock()
	t.handlers = append(t.handlers, handler)
}
