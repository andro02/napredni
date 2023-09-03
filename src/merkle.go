package src

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
)

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func NewNode(data [20]byte, left *Node, right *Node) *Node {

	node := Node{
		data:  data,
		left:  left,
		right: right,
	}
	return &node

}

type MerkleRoot struct {
	root *Node
}

func NewMerkleRoot(root *Node) *MerkleRoot {

	merkleRoot := MerkleRoot{
		root: root,
	}
	return &merkleRoot

}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func GenerateMerkleRoot(hashes [][20]byte) [20]byte {

	if len(hashes)%2 != 0 {
		hashes = append(hashes, hashes[len(hashes)-1])
	}

	combinedHashes := make([][20]byte, 0)
	for i := 0; i < len(hashes); i += 2 {
		hashPair := append(hashes[i][:], hashes[i+1][:]...)
		hash := Hash(hashPair)
		combinedHashes = append(combinedHashes, hash)
	}

	if len(combinedHashes) == 1 {
		return combinedHashes[0]
	}

	return GenerateMerkleRoot(combinedHashes)

}

func GenerateMerkleTree(hashes [][20]byte) *MerkleRoot {

	leaves := make([]*Node, 0)
	for i := 0; i < len(hashes); i++ {
		leaves = append(leaves, NewNode(hashes[i], nil, nil))
	}
	if len(leaves)%2 == 1 {
		leaves = append(leaves, leaves[len(leaves)-1])
	}
	return NewMerkleRoot(GenerateMerkleTreeRecursive(leaves))

}

func GenerateMerkleTreeRecursive(nodes []*Node) *Node {

	if len(nodes)%2 == 1 {
		lastNode := nodes[len(nodes)-1]
		nodes = append(nodes, NewNode(lastNode.data, nil, nil))
	}

	if len(nodes) == 2 {
		hash := Hash(append(nodes[0].data[:], nodes[1].data[:]...))
		return NewNode(hash, nodes[0], nodes[1])
	}

	nextNodes := make([]*Node, 0)
	for i := 0; i < len(nodes); i += 2 {
		hash := Hash(append(nodes[i].data[:], nodes[i+1].data[:]...))
		node := NewNode(hash, nodes[i], nodes[i+1])
		nextNodes = append(nextNodes, node)
	}
	return GenerateMerkleTreeRecursive(nextNodes)

}

func PrintMerkle(node *Node, level int) {

	fmt.Println("node: ", node)
	if node.left != nil {
		fmt.Println(level, ": ", "Left", ": ", node.left.data)
		fmt.Println(level, ": ", "Left", ": ", node.left)
		fmt.Println(level, ": ", "Right", ": ", node.right.data)
		fmt.Println(level, ": ", "Right", ": ", node.right)
		PrintMerkle(node.left, level+1)
		PrintMerkle(node.right, level+1)
	}

}

func CreateMerkle(data [][]byte) *MerkleRoot {

	hashes := make([][20]byte, 0)
	for _, element := range data {
		hashes = append(hashes, Hash(element))
	}
	merkleRoot := GenerateMerkleTree(hashes)
	return merkleRoot

}

func (merkle *MerkleRoot) WriteMetadata(path string) {

	metadataFile, err := os.Create(path + "metadata.txt")
	if err != nil {
		panic(err)
	}
	node := merkle.root
	metadataFile.WriteString(node.String())
	node.WriteNodeChildren(metadataFile)

}

func (node *Node) WriteNodeChildren(metadataFile *os.File) {

	if node.left != nil {
		metadataFile.WriteString(node.left.String())
	}
	if node.right != nil {
		metadataFile.WriteString(node.left.String())
	}

	if node.left != nil {
		node.left.WriteNodeChildren(metadataFile)
	}

	if node.right != nil {
		node.right.WriteNodeChildren(metadataFile)
	}

}

// func Test() {

// 	hashes := make([][20]byte, 0)
// 	for i := 0; i < 5; i++ {
// 		hashes = append(hashes, Hash([]byte("string"+strconv.Itoa(i))))
// 	}
// 	result := GenerateMerkleRoot(hashes)
// 	root := GenerateMerkleTree(hashes)
// 	fmt.Println(result)
// 	fmt.Println(root)

// }
