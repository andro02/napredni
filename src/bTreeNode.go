package src

type KeyValuePair struct {
	Key   string
	Value []byte
}

func NewKeyValuePair(key string, value []byte) *KeyValuePair {

	keyValuePair := KeyValuePair{
		Key:   key,
		Value: value,
	}
	return &keyValuePair

}

type BTreeNode struct {
	Leaf  bool
	Data  []*KeyValuePair
	Child []*BTreeNode
}

func NewBTreeNode(leaf bool) *BTreeNode {

	bTreeNode := BTreeNode{
		Leaf:  leaf,
		Data:  make([]*KeyValuePair, 0),
		Child: make([]*BTreeNode, 0),
	}
	return &bTreeNode

}
