// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package v2

import (
	"errors"
	"fmt"
	"github.com/mikkyang/id3-go/encodedbytes"
)

const (
	FrameHeaderSize = 10
)

// FrameType holds frame id metadata and constructor method
// A set number of these are created in the version specific files
type FrameType struct {
	id          string
	description string
	constructor func(FrameHead, []byte) Framer
}

// Framer provides a generic interface for frames
// This is the default type returned when creating frames
type Framer interface {
	Id() string
	Size() uint
	StatusFlags() byte
	FormatFlags() byte
	String() string
	Bytes() []byte
	setOwner(*Tag)
}

// FrameHead represents the header of each frame
// Additional metadata is kept through the embedded frame type
// These do not usually need to be manually created
type FrameHead struct {
	FrameType
	statusFlags byte
	formatFlags byte
	size        uint32
	owner       *Tag
}

func (ft FrameType) Id() string {
	return ft.id
}

func (h FrameHead) Size() uint {
	return uint(h.size)
}

func (h *FrameHead) changeSize(diff int) {
	if diff >= 0 {
		h.size += uint32(diff)
	} else {
		h.size -= uint32(-diff)
	}

	if h.owner != nil {
		h.owner.changeSize(diff)
	}
}

func (h FrameHead) StatusFlags() byte {
	return h.statusFlags
}

func (h FrameHead) FormatFlags() byte {
	return h.formatFlags
}

func (h *FrameHead) setOwner(t *Tag) {
	h.owner = t
}

// DataFrame is the default frame for binary data
type DataFrame struct {
	FrameHead
	data []byte
}

func NewDataFrame(ft FrameType, data []byte) *DataFrame {
	head := FrameHead{
		FrameType: ft,
		size:      uint32(len(data)),
	}

	return &DataFrame{head, data}
}

func ParseDataFrame(head FrameHead, data []byte) Framer {
	return &DataFrame{head, data}
}

func (f DataFrame) Data() []byte {
	return f.data
}

func (f *DataFrame) SetData(b []byte) {
	diff := len(b) - len(f.data)
	f.changeSize(diff)
	f.data = b
}

func (f DataFrame) String() string {
	return "<binary data>"
}

func (f DataFrame) Bytes() []byte {
	return f.data
}

// IdFrame represents identification tags
type IdFrame struct {
	FrameHead
	ownerIdentifier string
	identifier      []byte
}

func NewIdFrame(ft FrameType, ownerId string, id []byte) *IdFrame {
	head := FrameHead{
		FrameType: ft,
		size:      uint32(1 + len(ownerId) + len(id)),
	}

	return &IdFrame{
		FrameHead:       head,
		ownerIdentifier: ownerId,
		identifier:      id,
	}
}

func ParseIdFrame(head FrameHead, data []byte) Framer {
	var err error
	f := &IdFrame{FrameHead: head}
	rd := encodedbytes.NewReader(data)

	if f.ownerIdentifier, err = rd.ReadNullTermString(encodedbytes.NativeEncoding); err != nil {
		return nil
	}

	if f.identifier, err = rd.ReadRest(); len(f.identifier) > 64 || err != nil {
		return nil
	}

	return f
}

func (f IdFrame) OwnerIdentifier() string {
	return f.ownerIdentifier
}

func (f *IdFrame) SetOwnerIdentifier(ownerId string) {
	f.changeSize(len(ownerId) - len(f.ownerIdentifier))
	f.ownerIdentifier = ownerId
}

func (f IdFrame) Identifier() []byte {
	return f.identifier
}

func (f *IdFrame) SetIdentifier(id []byte) error {
	if len(id) > 64 {
		return errors.New("identifier: identifier too long")
	}

	f.changeSize(len(id) - len(f.identifier))
	f.identifier = id

	return nil
}

func (f IdFrame) String() string {
	return fmt.Sprintf("%s: %v", f.ownerIdentifier, f.identifier)
}

func (f IdFrame) Bytes() []byte {
	var err error
	bytes := make([]byte, f.Size())
	wr := encodedbytes.NewWriter(bytes)

	if err = wr.WriteString(f.ownerIdentifier, encodedbytes.NativeEncoding); err != nil {
		return bytes
	}

	if _, err = wr.Write(f.identifier); err != nil {
		return bytes
	}

	return bytes
}

// TextFramer represents frames that contain encoded text
type TextFramer interface {
	Framer
	Encoding() string
	SetEncoding(string) error
	Text() string
	SetText(string) error
}

// TextFrame represents frames that contain encoded text
type TextFrame struct {
	FrameHead
	encoding byte
	text     string
}

func NewTextFrame(ft FrameType, text string) *TextFrame {
	head := FrameHead{
		FrameType: ft,
		size:      uint32(1 + len(text)),
	}

	return &TextFrame{
		FrameHead: head,
		text:      text,
	}
}

func ParseTextFrame(head FrameHead, data []byte) Framer {
	var err error
	f := &TextFrame{FrameHead: head}
	rd := encodedbytes.NewReader(data)

	if f.encoding, err = rd.ReadByte(); err != nil {
		return nil
	}

	if f.text, err = rd.ReadRestString(f.encoding); err != nil {
		return nil
	}

	return f
}

func (f TextFrame) Encoding() string {
	return encodedbytes.EncodingForIndex(f.encoding)
}

func (f *TextFrame) SetEncoding(encoding string) error {
	i := byte(encodedbytes.IndexForEncoding(encoding))
	if i < 0 {
		return errors.New("encoding: invalid encoding")
	}

	diff, err := encodedbytes.EncodedDiff(i, f.text, f.encoding, f.text)
	if err != nil {
		return err
	}

	f.changeSize(diff)
	f.encoding = i
	return nil
}

func (f TextFrame) Text() string {
	return f.text
}

func (f *TextFrame) SetText(text string) error {
	diff, err := encodedbytes.EncodedDiff(f.encoding, text, f.encoding, f.text)
	if err != nil {
		return err
	}

	f.changeSize(diff)
	f.text = text
	return nil
}

func (f TextFrame) String() string {
	return f.text
}

func (f TextFrame) Bytes() []byte {
	var err error
	bytes := make([]byte, f.Size())
	wr := encodedbytes.NewWriter(bytes)

	if err = wr.WriteByte(f.encoding); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.text, f.encoding); err != nil {
		return bytes
	}

	return bytes
}

type DescTextFrame struct {
	TextFrame
	description string
}

func NewDescTextFrame(ft FrameType, desc, text string) *DescTextFrame {
	f := NewTextFrame(ft, text)
	nullLength := encodedbytes.EncodingNullLengthForIndex(f.encoding)
	f.size += uint32(len(desc) + nullLength)

	return &DescTextFrame{
		TextFrame:   *f,
		description: desc,
	}
}

// DescTextFrame represents frames that contain encoded text and descriptions
func ParseDescTextFrame(head FrameHead, data []byte) Framer {
	var err error
	f := new(DescTextFrame)
	f.FrameHead = head
	rd := encodedbytes.NewReader(data)

	if f.encoding, err = rd.ReadByte(); err != nil {
		return nil
	}

	if f.description, err = rd.ReadNullTermString(f.encoding); err != nil {
		return nil
	}

	if f.text, err = rd.ReadRestString(f.encoding); err != nil {
		return nil
	}

	return f
}

func (f DescTextFrame) Description() string {
	return f.description
}

func (f *DescTextFrame) SetDescription(description string) error {
	diff, err := encodedbytes.EncodedDiff(f.encoding, description, f.encoding, f.description)
	if err != nil {
		return err
	}

	f.changeSize(diff)
	f.description = description
	return nil
}

func (f *DescTextFrame) SetEncoding(encoding string) error {
	i := byte(encodedbytes.IndexForEncoding(encoding))
	if i < 0 {
		return errors.New("encoding: invalid encoding")
	}

	descDiff, err := encodedbytes.EncodedDiff(i, f.text, f.encoding, f.text)
	if err != nil {
		return err
	}

	newNullLength := encodedbytes.EncodingNullLengthForIndex(i)
	oldNullLength := encodedbytes.EncodingNullLengthForIndex(f.encoding)
	nullDiff := newNullLength - oldNullLength

	textDiff, err := encodedbytes.EncodedDiff(i, f.description, f.encoding, f.description)
	if err != nil {
		return err
	}

	f.changeSize(descDiff + nullDiff + textDiff)
	f.encoding = i
	return nil
}

func (f DescTextFrame) String() string {
	return fmt.Sprintf("%s: %s", f.description, f.text)
}

func (f DescTextFrame) Bytes() []byte {
	var err error
	bytes := make([]byte, f.Size())
	wr := encodedbytes.NewWriter(bytes)

	if err = wr.WriteByte(f.encoding); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.description, f.encoding); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.text, f.encoding); err != nil {
		return bytes
	}

	return bytes
}

// UnsynchTextFrame represents frames that contain unsynchronized text
type UnsynchTextFrame struct {
	DescTextFrame
	language string
}

func NewUnsynchTextFrame(ft FrameType, desc, text string) *UnsynchTextFrame {
	f := NewDescTextFrame(ft, desc, text)
	f.size += uint32(3)

	return &UnsynchTextFrame{
		DescTextFrame: *f,
		language:      "eng",
	}
}

func ParseUnsynchTextFrame(head FrameHead, data []byte) Framer {
	var err error
	f := new(UnsynchTextFrame)
	f.FrameHead = head
	rd := encodedbytes.NewReader(data)

	if f.encoding, err = rd.ReadByte(); err != nil {
		return nil
	}

	if f.language, err = rd.ReadNumBytesString(3); err != nil {
		return nil
	}

	if f.description, err = rd.ReadNullTermString(f.encoding); err != nil {
		return nil
	}

	if f.text, err = rd.ReadRestString(f.encoding); err != nil {
		return nil
	}

	return f
}

func (f UnsynchTextFrame) Language() string {
	return f.language
}

func (f *UnsynchTextFrame) SetLanguage(language string) error {
	if len(language) != 3 {
		return errors.New("language: invalid language string")
	}

	f.language = language
	f.changeSize(0)
	return nil
}

func (f UnsynchTextFrame) String() string {
	return fmt.Sprintf("%s\t%s:\n%s", f.language, f.description, f.text)
}

func (f UnsynchTextFrame) Bytes() []byte {
	var err error
	bytes := make([]byte, f.Size())
	wr := encodedbytes.NewWriter(bytes)

	if err = wr.WriteByte(f.encoding); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.language, encodedbytes.NativeEncoding); err != nil {
		return bytes
	}

	if err = wr.WriteNullTermString(f.description, f.encoding); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.text, f.encoding); err != nil {
		return bytes
	}

	return bytes
}

// ImageFrame represent frames that have media attached
type ImageFrame struct {
	DataFrame
	encoding    byte
	mimeType    string
	pictureType byte
	description string
}

func ParseImageFrame(head FrameHead, data []byte) Framer {
	var err error
	f := new(ImageFrame)
	f.FrameHead = head
	rd := encodedbytes.NewReader(data)

	if f.encoding, err = rd.ReadByte(); err != nil {
		return nil
	}

	if f.mimeType, err = rd.ReadNullTermString(encodedbytes.NativeEncoding); err != nil {
		return nil
	}

	if f.pictureType, err = rd.ReadByte(); err != nil {
		return nil
	}

	if f.description, err = rd.ReadNullTermString(f.encoding); err != nil {
		return nil
	}

	if f.data, err = rd.ReadRest(); err != nil {
		return nil
	}

	return f
}

func (f ImageFrame) Encoding() string {
	return encodedbytes.EncodingForIndex(f.encoding)
}

func (f *ImageFrame) SetEncoding(encoding string) error {
	i := byte(encodedbytes.IndexForEncoding(encoding))
	if i < 0 {
		return errors.New("encoding: invalid encoding")
	}

	diff, err := encodedbytes.EncodedDiff(i, f.description, f.encoding, f.description)
	if err != nil {
		return err
	}

	f.changeSize(diff)
	f.encoding = i
	return nil
}

func (f ImageFrame) MIMEType() string {
	return f.mimeType
}

func (f *ImageFrame) SetMIMEType(mimeType string) {
	diff := len(mimeType) - len(f.mimeType)
	if mimeType[len(mimeType)-1] != 0 {
		nullTermBytes := append([]byte(mimeType), 0x00)
		f.mimeType = string(nullTermBytes)
		diff += 1
	} else {
		f.mimeType = mimeType
	}

	f.changeSize(diff)
}

func (f ImageFrame) String() string {
	return fmt.Sprintf("%s\t%s: <binary data>", f.mimeType, f.description)
}

func (f ImageFrame) Bytes() []byte {
	var err error
	bytes := make([]byte, f.Size())
	wr := encodedbytes.NewWriter(bytes)

	if err = wr.WriteByte(f.encoding); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.mimeType, encodedbytes.NativeEncoding); err != nil {
		return bytes
	}

	if err = wr.WriteByte(f.pictureType); err != nil {
		return bytes
	}

	if err = wr.WriteString(f.description, f.encoding); err != nil {
		return bytes
	}

	if n, err := wr.Write(f.data); n < len(f.data) || err != nil {
		return bytes
	}

	return bytes
}
