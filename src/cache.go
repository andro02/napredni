package src

import (
	"container/list"

	config "github.com/andro02/napredni/config"
)

type Element struct {
	key []byte
	value ElementData
}

type CacheEntry struct {
	key       []byte
	value     []byte
	timestamp int64
}

func (element *CacheEntry) Value() []byte {
	return element.value
}
func (element *CacheEntry) Key() []byte {
	return element.key
}
func (element *CacheEntry) Timestamp() int64 {
	return element.timestamp
}
func (element *CacheEntry) SetKey(key []byte) {
	element.key = key
}
func (element *CacheEntry) SetValue(value []byte) {
	element.value = value
}
func (element *CacheEntry) SetTimestamp(timestamp int64) {
	element.timestamp = timestamp
}

type LRUCache struct{
	doubly_linked_list *list.List
	capacity int
	elements map[string]*list.Element
}

func Init(){
	NewLRUCache(config.CACHE_SIZE)
}

func NewLRUCache(capacity int) *LRUCache{
	//dodaj validaciju za capacity
	l := &LRUCache{
		doubly_linked_list: list.New(),
		capacity: capacity,
		elements: make(map[string]*list.Element),
	}
	return l
}

func (l *LRUCache) Put(element ElementData) {
	if item, valid := l.elements[string(element.Key())]; !valid {
		if l.capacity == len(l.elements) {
			pop_up := l.doubly_linked_list.Back()
			l.doubly_linked_list.Remove(pop_up)
			el := pop_up.Value.(*Element)
			delete(l.elements, string(el.value.Key()))
		}

	} else {

		l.doubly_linked_list.Remove(item)
		el := item.Value.(*Element)
		delete(l.elements, string(el.value.Key()))
	}

	newElement := &Element{key: element.Key(), value: element}
	el_reference := l.doubly_linked_list.PushFront(newElement)
	l.elements[string(newElement.key)] = el_reference

}

func (l *LRUCache) Get(key []byte) (ElementData, bool) {
	itemfound, valid := l.elements[string(key)] 
	if valid {
		l.doubly_linked_list.MoveToFront(itemfound)
		el := itemfound.Value.(*Element)
		return el.value, true
	}
	return nil, false
}

// func main(){
// 	cache := NewLRUCache(3)


// 	element1 := &CacheEntry{key: []byte("key1"), value: []byte("value1"), timestamp: 1}
//     element2 := &CacheEntry{key: []byte("key2"), value: []byte("value2"), timestamp: 2}
//     element3 := &CacheEntry{key: []byte("key3"), value: []byte("value3"), timestamp: 3}
//     element4 := &CacheEntry{key: []byte("key4"), value: []byte("value4"), timestamp: 4}


// 	cache.Put(element1)
// 	cache.Put(element2)
// 	cache.Put(element3)


// 	if _, found := cache.Get([]byte("key1")); found {
// 		fmt.Println("Key1 found in cache")
// 	} else {
// 		fmt.Println("Key1 not found in cache")
// 	}


// 	cache.Put(element4)

// 	if _, found := cache.Get([]byte("key2")); found {
// 		fmt.Println("Key2 found in cache")
// 	} else {
// 		fmt.Println("Key2 not found in cache")
// 	}
// }

