package io

import (
	"context"
	"errors"
	"fmt"
	"io"

	mdag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	ftpb "github.com/ipfs/go-ipfs/unixfs/pb"

	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
)

// DagReader provides a way to easily read the data contained in a dag.
type pbDagReader struct {
	serv mdag.DAGService

	// the node being read
	node *mdag.ProtoNode

	// cached protobuf structure from node.Data
	pbdata *ftpb.Data

	// the current data buffer to be read from
	// will either be a bytes.Reader or a child DagReader
	buf ReadSeekCloser

	// NodeGetters for each of 'nodes' child links
	promises []mdag.NodeGetter

	// the index of the child link currently being read from
	linkPosition int

	// current offset for the read head within the 'file'
	offset int64

	// Our context
	ctx context.Context

	// context cancel for children
	cancel func()
}

var _ DagReader = (*pbDagReader)(nil)

func NewPBFileReader(ctx context.Context, n *mdag.ProtoNode, pb *ftpb.Data, serv mdag.DAGService) *pbDagReader {
	fctx, cancel := context.WithCancel(ctx)
	promises := mdag.GetDAG(fctx, serv, n)
	return &pbDagReader{
		node:     n,
		serv:     serv,
		buf:      NewBufDagReader(pb.GetData()),
		promises: promises,
		ctx:      fctx,
		cancel:   cancel,
		pbdata:   pb,
	}
}

// precalcNextBuf follows the next link in line and loads it from the
// DAGService, setting the next buffer to read from
func (dr *pbDagReader) precalcNextBuf(ctx context.Context) error {
	dr.buf.Close() // Just to make sure
	if dr.linkPosition >= len(dr.promises) {
		return io.EOF
	}

	nxt, err := dr.promises[dr.linkPosition].Get(ctx)
	if err != nil {
		return err
	}
	dr.linkPosition++

	switch nxt := nxt.(type) {
	case *mdag.ProtoNode:
		pb := new(ftpb.Data)
		err = proto.Unmarshal(nxt.Data(), pb)
		if err != nil {
			return fmt.Errorf("incorrectly formatted protobuf: %s", err)
		}

		switch pb.GetType() {
		case ftpb.Data_Directory:
			// A directory should not exist within a file
			return ft.ErrInvalidDirLocation
		case ftpb.Data_File:
			dr.buf = NewPBFileReader(dr.ctx, nxt, pb, dr.serv)
			return nil
		case ftpb.Data_Raw:
			dr.buf = NewBufDagReader(pb.GetData())
			return nil
		case ftpb.Data_Metadata:
			return errors.New("shouldnt have had metadata object inside file")
		case ftpb.Data_Symlink:
			return errors.New("shouldnt have had symlink inside file")
		default:
			return ft.ErrUnrecognizedType
		}
	default:
		var err error
		dr.buf, err = NewDagReader(ctx, nxt, dr.serv)
		return err
	}
}

// Size return the total length of the data from the DAG structured file.
func (dr *pbDagReader) Size() uint64 {
	return dr.pbdata.GetFilesize()
}

// Read reads data from the DAG structured file
func (dr *pbDagReader) Read(b []byte) (int, error) {
	return dr.CtxReadFull(dr.ctx, b)
}

// CtxReadFull reads data from the DAG structured file
func (dr *pbDagReader) CtxReadFull(ctx context.Context, b []byte) (int, error) {
	// If no cached buffer, load one
	total := 0
	for {
		// Attempt to fill bytes from cached buffer
		n, err := dr.buf.Read(b[total:])
		total += n
		dr.offset += int64(n)
		if err != nil {
			// EOF is expected
			if err != io.EOF {
				return total, err
			}
		}

		// If weve read enough bytes, return
		if total == len(b) {
			return total, nil
		}

		// Otherwise, load up the next block
		err = dr.precalcNextBuf(ctx)
		if err != nil {
			return total, err
		}
	}
}

func (dr *pbDagReader) WriteTo(w io.Writer) (int64, error) {
	// If no cached buffer, load one
	total := int64(0)
	for {
		// Attempt to write bytes from cached buffer
		n, err := dr.buf.WriteTo(w)
		total += n
		dr.offset += n
		if err != nil {
			if err != io.EOF {
				return total, err
			}
		}

		// Otherwise, load up the next block
		err = dr.precalcNextBuf(dr.ctx)
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

func (dr *pbDagReader) Close() error {
	dr.cancel()
	return nil
}

func (dr *pbDagReader) Offset() int64 {
	return dr.offset
}

// Seek implements io.Seeker, and will seek to a given offset in the file
// interface matches standard unix seek
// TODO: check if we can do relative seeks, to reduce the amount of dagreader
// recreations that need to happen.
func (dr *pbDagReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return -1, errors.New("Invalid offset")
		}
		if offset == dr.offset {
			return offset, nil
		}

		// Grab cached protobuf object (solely to make code look cleaner)
		pb := dr.pbdata

		// left represents the number of bytes remaining to seek to (from beginning)
		left := offset
		if int64(len(pb.Data)) >= offset {
			// Close current buf to close potential child dagreader
			dr.buf.Close()
			dr.buf = NewBufDagReader(pb.GetData()[offset:])

			// start reading links from the beginning
			dr.linkPosition = 0
			dr.offset = offset
			return offset, nil
		} else {
			// skip past root block data
			left -= int64(len(pb.Data))
		}

		// iterate through links and find where we need to be
		for i := 0; i < len(pb.Blocksizes); i++ {
			if pb.Blocksizes[i] > uint64(left) {
				dr.linkPosition = i
				break
			} else {
				left -= int64(pb.Blocksizes[i])
			}
		}

		// start sub-block request
		err := dr.precalcNextBuf(dr.ctx)
		if err != nil {
			return 0, err
		}

		// set proper offset within child readseeker
		n, err := dr.buf.Seek(left, io.SeekStart)
		if err != nil {
			return -1, err
		}

		// sanity
		left -= n
		if left != 0 {
			return -1, errors.New("failed to seek properly")
		}
		dr.offset = offset
		return offset, nil
	case io.SeekCurrent:
		// TODO: be smarter here
		if offset == 0 {
			return dr.offset, nil
		}

		noffset := dr.offset + offset
		return dr.Seek(noffset, io.SeekStart)
	case io.SeekEnd:
		noffset := int64(dr.pbdata.GetFilesize()) - offset
		n, err := dr.Seek(noffset, io.SeekStart)

		// Return negative number if we can't figure out the file size. Using io.EOF
		// for this seems to be good(-enough) solution as it's only returned by
		// precalcNextBuf when we step out of file range.
		// This is needed for gateway to function properly
		if err == io.EOF && *dr.pbdata.Type == ftpb.Data_File {
			return -1, nil
		}
		return n, err
	default:
		return 0, errors.New("invalid whence")
	}
}
