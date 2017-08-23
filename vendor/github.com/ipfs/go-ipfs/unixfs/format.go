// Package format implements a data format for files in the IPFS filesystem It
// is not the only format in ipfs, but it is the one that the filesystem
// assumes
package unixfs

import (
	"errors"

	dag "github.com/ipfs/go-ipfs/merkledag"
	pb "github.com/ipfs/go-ipfs/unixfs/pb"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
)

const (
	TRaw       = pb.Data_Raw
	TFile      = pb.Data_File
	TDirectory = pb.Data_Directory
	TMetadata  = pb.Data_Metadata
	TSymlink   = pb.Data_Symlink
	THAMTShard = pb.Data_HAMTShard
)

var ErrMalformedFileFormat = errors.New("malformed data in file format")
var ErrInvalidDirLocation = errors.New("found directory node in unexpected place")
var ErrUnrecognizedType = errors.New("unrecognized node type")

func FromBytes(data []byte) (*pb.Data, error) {
	pbdata := new(pb.Data)
	err := proto.Unmarshal(data, pbdata)
	if err != nil {
		return nil, err
	}
	return pbdata, nil
}

func FilePBData(data []byte, totalsize uint64) []byte {
	pbfile := new(pb.Data)
	typ := pb.Data_File
	pbfile.Type = &typ
	pbfile.Data = data
	pbfile.Filesize = proto.Uint64(totalsize)

	data, err := proto.Marshal(pbfile)
	if err != nil {
		// This really shouldnt happen, i promise
		// The only failure case for marshal is if required fields
		// are not filled out, and they all are. If the proto object
		// gets changed and nobody updates this function, the code
		// should panic due to programmer error
		panic(err)
	}
	return data
}

//FolderPBData returns Bytes that represent a Directory.
func FolderPBData() []byte {
	pbfile := new(pb.Data)
	typ := pb.Data_Directory
	pbfile.Type = &typ

	data, err := proto.Marshal(pbfile)
	if err != nil {
		//this really shouldnt happen, i promise
		panic(err)
	}
	return data
}

//WrapData marshals raw bytes into a `Data_Raw` type protobuf message.
func WrapData(b []byte) []byte {
	pbdata := new(pb.Data)
	typ := pb.Data_Raw
	pbdata.Data = b
	pbdata.Type = &typ
	pbdata.Filesize = proto.Uint64(uint64(len(b)))

	out, err := proto.Marshal(pbdata)
	if err != nil {
		// This shouldnt happen. seriously.
		panic(err)
	}

	return out
}

//SymlinkData returns a `Data_Symlink` protobuf message for the path you specify.
func SymlinkData(path string) ([]byte, error) {
	pbdata := new(pb.Data)
	typ := pb.Data_Symlink
	pbdata.Data = []byte(path)
	pbdata.Type = &typ

	out, err := proto.Marshal(pbdata)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func UnwrapData(data []byte) ([]byte, error) {
	pbdata := new(pb.Data)
	err := proto.Unmarshal(data, pbdata)
	if err != nil {
		return nil, err
	}
	return pbdata.GetData(), nil
}

func DataSize(data []byte) (uint64, error) {
	pbdata := new(pb.Data)
	err := proto.Unmarshal(data, pbdata)
	if err != nil {
		return 0, err
	}

	switch pbdata.GetType() {
	case pb.Data_Directory:
		return 0, errors.New("Cant get data size of directory!")
	case pb.Data_File:
		return pbdata.GetFilesize(), nil
	case pb.Data_Raw:
		return uint64(len(pbdata.GetData())), nil
	default:
		return 0, errors.New("Unrecognized node data type!")
	}
}

type FSNode struct {
	Data []byte

	// total data size for each child
	blocksizes []uint64

	// running sum of blocksizes
	subtotal uint64

	// node type of this node
	Type pb.Data_DataType
}

func FSNodeFromBytes(b []byte) (*FSNode, error) {
	pbn := new(pb.Data)
	err := proto.Unmarshal(b, pbn)
	if err != nil {
		return nil, err
	}

	n := new(FSNode)
	n.Data = pbn.Data
	n.blocksizes = pbn.Blocksizes
	n.subtotal = pbn.GetFilesize() - uint64(len(n.Data))
	n.Type = pbn.GetType()
	return n, nil
}

// AddBlockSize adds the size of the next child block of this node
func (n *FSNode) AddBlockSize(s uint64) {
	n.subtotal += s
	n.blocksizes = append(n.blocksizes, s)
}

func (n *FSNode) RemoveBlockSize(i int) {
	n.subtotal -= n.blocksizes[i]
	n.blocksizes = append(n.blocksizes[:i], n.blocksizes[i+1:]...)
}

func (n *FSNode) GetBytes() ([]byte, error) {
	pbn := new(pb.Data)
	pbn.Type = &n.Type
	pbn.Filesize = proto.Uint64(uint64(len(n.Data)) + n.subtotal)
	pbn.Blocksizes = n.blocksizes
	pbn.Data = n.Data
	return proto.Marshal(pbn)
}

func (n *FSNode) FileSize() uint64 {
	return uint64(len(n.Data)) + n.subtotal
}

func (n *FSNode) NumChildren() int {
	return len(n.blocksizes)
}

type Metadata struct {
	MimeType string
	Size     uint64
}

//MetadataFromBytes Unmarshals a protobuf message into Metadata.
func MetadataFromBytes(b []byte) (*Metadata, error) {
	pbd := new(pb.Data)
	err := proto.Unmarshal(b, pbd)
	if err != nil {
		return nil, err
	}
	if pbd.GetType() != pb.Data_Metadata {
		return nil, errors.New("incorrect node type")
	}

	pbm := new(pb.Metadata)
	err = proto.Unmarshal(pbd.Data, pbm)
	if err != nil {
		return nil, err
	}
	md := new(Metadata)
	md.MimeType = pbm.GetMimeType()
	return md, nil
}

func (m *Metadata) Bytes() ([]byte, error) {
	pbm := new(pb.Metadata)
	pbm.MimeType = &m.MimeType
	return proto.Marshal(pbm)
}

func BytesForMetadata(m *Metadata) ([]byte, error) {
	pbd := new(pb.Data)
	pbd.Filesize = proto.Uint64(m.Size)
	typ := pb.Data_Metadata
	pbd.Type = &typ
	mdd, err := m.Bytes()
	if err != nil {
		return nil, err
	}

	pbd.Data = mdd
	return proto.Marshal(pbd)
}

func EmptyDirNode() *dag.ProtoNode {
	return dag.NodeWithData(FolderPBData())
}
