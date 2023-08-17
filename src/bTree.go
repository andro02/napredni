package src

import (
	"fmt"
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

func (bTree *BTree) Insert(key int, value []byte) {

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

	if x.Leaf == true {

		x.Data = append(x.Data, nil)
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
		bTree.InsertNonFull(x.Child[i], k)

	}

}

func (bTree *BTree) SplitChild(x *BTreeNode, index int) {

	y := x.Child[index]
	z := NewBTreeNode(y.Leaf)

	if len(x.Child) <= index+1 {
		x.Child = append(x.Child, nil)
	}
	x.Child[index+1] = z

	if len(x.Data) <= index {
		x.Data = append(x.Data, nil)
	}
	x.Data[index] = y.Data[bTree.Limit-1]

	z.Data = y.Data[bTree.Limit : 2*bTree.Limit-1]
	y.Data = y.Data[0 : bTree.Limit-1]
	if y.Leaf == false {
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

func (bTree *BTree) SearchKey(key int, x *BTreeNode) (*BTreeNode, int) {

	i := 0
	for i < len(x.Data) && key > x.Data[i].Key {
		i += 1
	}
	if i < len(x.Data) && key == x.Data[i].Key {
		return x, i
	} else if x.Leaf == true {
		return nil, -1
	} else {
		return bTree.SearchKey(key, x.Child[i])
	}

}

// func Test() {

// 	B := NewBTree()

// 	for i := 0; i < 20; i++ {
// 		B.Insert(1, []byte("vrednost"))
// 		B.PrintTree(B.Root, 0)
// 		fmt.Println("-----------")
// 	}
// 	for i := 0; i < 10; i++ {
// 		//_, index := B.SearchKey(-1, B.Root)
// 		//fmt.Println(node, index)
// 		// if index == -1 {
// 		// 	fmt.Print("greska")
// 		// }
// 		//B.PrintTree(B.Root, 0)
// 	}

// }
