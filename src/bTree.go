package src

import (
	"fmt"
	"strconv"
)

type BTree struct {
	Root  *BTreeNode
	Limit int
}

func NewBTree() *BTree {

	bTree := BTree{
		Root:  NewBTreeNode(true),
		Limit: 3,
	}
	return &bTree

}

func (bTree *BTree) Insert(key string, value []byte) {

	_, found := bTree.SearchKey(key, bTree.Root)

	if found != -1 {
		fmt.Println("Key already exists. Error.")
		return
	}

	k := NewKeyValuePair(key, value)
	root := bTree.Root

	if len(root.Data) == (2*bTree.Limit)-1 {
		temp := NewBTreeNode(false)
		bTree.Root = temp
		if len(temp.Child) == 0 {
			temp.Child = append(temp.Child, nil)
		}
		temp.Child[0] = root
		bTree.SplitChild(temp, 0)
		bTree.InsertNonFull(temp, k)
	} else {
		bTree.InsertNonFull(root, k)
	}

}
func (bTree *BTree) InsertNonFull(x *BTreeNode, k *KeyValuePair) {

	i := len(x.Data) - 1

	if x.Leaf {

		oldData := x.Data
		x.Data = make([]*KeyValuePair, len(oldData)+1)
		copy(x.Data, oldData)
		for i >= 0 && k.Key < x.Data[i].Key {
			x.Data[i+1] = x.Data[i]
			i -= 1
		}
		x.Data[i+1] = k

	} else {

		for i >= 0 && k.Key < x.Data[i].Key {
			i -= 1
		}
		i += 1

		if len(x.Child[i].Data) == (2*bTree.Limit)-1 {
			bTree.SplitChild(x, i)
			if k.Key > x.Data[i].Key {
				i += 1
			}
		}
		if k.Key == strconv.Itoa(11) {
			fmt.Print()
		}
		bTree.InsertNonFull(x.Child[i], k)

	}

}

func (bTree *BTree) SplitChild(x *BTreeNode, index int) {

	y := x.Child[index]
	z := NewBTreeNode(y.Leaf)

	oldChild := x.Child
	x.Child = make([]*BTreeNode, len(oldChild)+1)
	copy(x.Child, oldChild)
	for i := len(x.Child) - 1; i > index+1; i-- {
		x.Child[i] = x.Child[i-1]
	}
	x.Child[index+1] = z

	oldData := x.Data
	x.Data = make([]*KeyValuePair, len(oldData)+1)
	copy(x.Data, oldData)
	for i := len(x.Data) - 1; i > index; i-- {
		x.Data[i] = x.Data[i-1]
	}
	x.Data[index] = y.Data[bTree.Limit-1]

	z.Data = y.Data[bTree.Limit : 2*bTree.Limit-1]
	y.Data = y.Data[0 : bTree.Limit-1]
	if !y.Leaf {
		z.Child = y.Child[bTree.Limit : 2*bTree.Limit]
		y.Child = y.Child[0:bTree.Limit]
	}

}

func (bTree *BTree) PrintTree(x *BTreeNode, l int) {

	fmt.Print("Level ", l, " ", len(x.Data), ":")
	for _, value := range x.Data {
		fmt.Print(value.Key, ", ")
	}
	fmt.Println()
	l += 1
	if len(x.Child) > 0 {
		for _, value := range x.Child {
			bTree.PrintTree(value, l)
		}
	}

}

func (bTree *BTree) SearchKey(key string, x *BTreeNode) (*BTreeNode, int) {

	i := 0
	for i < len(x.Data) && key > x.Data[i].Key {
		i += 1
	}
	if i < len(x.Data) && key == x.Data[i].Key {
		return x, i
	} else if x.Leaf {
		return nil, -1
	} else {
		return bTree.SearchKey(key, x.Child[i])
	}

}

func (bTree *BTree) GetAllElements() []*KeyValuePair {

	elements := make([]*KeyValuePair, 0)
	elements = bTree.GetAllElementsRecursive(bTree.Root, elements)
	return elements

}

func (bTree *BTree) GetAllElementsRecursive(node *BTreeNode, elements []*KeyValuePair) []*KeyValuePair {

	for i := 0; i < len(node.Child); i++ {
		elements = bTree.GetAllElementsRecursive(node.Child[i], elements)
		if i != len(node.Child)-1 {
			elements = append(elements, node.Data[i])
		}
	}
	if len(node.Child) == 0 {
		for i := 0; i < len(node.Data); i++ {
			elements = append(elements, node.Data[i])
		}
	}
	return elements

}

// func Test() {

// 	B := NewBTree()
// 	size := 100

// 	for i := 0; i < size; i++ {
// 		B.Insert(strconv.Itoa(i), []byte("vrednost"))
// 		//B.Insert(i, []byte("vrednost"))
// 		//B.PrintTree(B.Root, 0)
// 		//fmt.Println("-----------")
// 	}
// 	//B.PrintTree(B.Root, 0)
// 	elements := B.GetAllElements()
// 	fmt.Println(len(elements))
// 	greske := 0
// 	dobri := 0
// 	for i := 0; i < size; i++ {
// 		_, index := B.SearchKey(strconv.Itoa(i), B.Root)
// 		//_, index := B.SearchKey(i, B.Root)
// 		//fmt.Println(node, index)
// 		if index == -1 {
// 			greske++
// 			fmt.Println(i)
// 		} else {
// 			dobri++
// 		}
// 		//B.PrintTree(B.Root, 0)
// 	}
// 	fmt.Println("greske: ", greske, ", dobri: ", dobri)

// }
