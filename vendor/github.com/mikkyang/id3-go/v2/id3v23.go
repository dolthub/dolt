// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package v2

import (
	"github.com/mikkyang/id3-go/encodedbytes"
	"io"
)

var (
	// Common frame IDs
	V23CommonFrame = map[string]FrameType{
		"Title":    V23FrameTypeMap["TIT2"],
		"Artist":   V23FrameTypeMap["TPE1"],
		"Album":    V23FrameTypeMap["TALB"],
		"Year":     V23FrameTypeMap["TYER"],
		"Genre":    V23FrameTypeMap["TCON"],
		"Comments": V23FrameTypeMap["COMM"],
	}

	// V23DeprecatedTypeMap contains deprecated frame IDs from ID3v2.2
	V23DeprecatedTypeMap = map[string]string{
		"BUF": "RBUF", "COM": "COMM", "CRA": "AENC", "EQU": "EQUA",
		"ETC": "ETCO", "GEO": "GEOB", "MCI": "MCDI", "MLL": "MLLT",
		"PIC": "APIC", "POP": "POPM", "REV": "RVRB", "RVA": "RVAD",
		"SLT": "SYLT", "STC": "SYTC", "TAL": "TALB", "TBP": "TBPM",
		"TCM": "TCOM", "TCO": "TCON", "TCR": "TCOP", "TDA": "TDAT",
		"TDY": "TDLY", "TEN": "TENC", "TFT": "TFLT", "TIM": "TIME",
		"TKE": "TKEY", "TLA": "TLAN", "TLE": "TLEN", "TMT": "TMED",
		"TOA": "TOPE", "TOF": "TOFN", "TOL": "TOLY", "TOR": "TORY",
		"TOT": "TOAL", "TP1": "TPE1", "TP2": "TPE2", "TP3": "TPE3",
		"TP4": "TPE4", "TPA": "TPOS", "TPB": "TPUB", "TRC": "TSRC",
		"TRD": "TRDA", "TRK": "TRCK", "TSI": "TSIZ", "TSS": "TSSE",
		"TT1": "TIT1", "TT2": "TIT2", "TT3": "TIT3", "TXT": "TEXT",
		"TXX": "TXXX", "TYE": "TYER", "UFI": "UFID", "ULT": "USLT",
		"WAF": "WOAF", "WAR": "WOAR", "WAS": "WOAS", "WCM": "WCOM",
		"WCP": "WCOP", "WPB": "WPB", "WXX": "WXXX",
	}

	// V23FrameTypeMap specifies the frame IDs and constructors allowed in ID3v2.3
	V23FrameTypeMap = map[string]FrameType{
		"AENC": FrameType{id: "AENC", description: "Audio encryption", constructor: ParseDataFrame},
		"APIC": FrameType{id: "APIC", description: "Attached picture", constructor: ParseImageFrame},
		"COMM": FrameType{id: "COMM", description: "Comments", constructor: ParseUnsynchTextFrame},
		"COMR": FrameType{id: "COMR", description: "Commercial frame", constructor: ParseDataFrame},
		"ENCR": FrameType{id: "ENCR", description: "Encryption method registration", constructor: ParseDataFrame},
		"EQUA": FrameType{id: "EQUA", description: "Equalization", constructor: ParseDataFrame},
		"ETCO": FrameType{id: "ETCO", description: "Event timing codes", constructor: ParseDataFrame},
		"GEOB": FrameType{id: "GEOB", description: "General encapsulated object", constructor: ParseDataFrame},
		"GRID": FrameType{id: "GRID", description: "Group identification registration", constructor: ParseDataFrame},
		"IPLS": FrameType{id: "IPLS", description: "Involved people list", constructor: ParseDataFrame},
		"LINK": FrameType{id: "LINK", description: "Linked information", constructor: ParseDataFrame},
		"MCDI": FrameType{id: "MCDI", description: "Music CD identifier", constructor: ParseDataFrame},
		"MLLT": FrameType{id: "MLLT", description: "MPEG location lookup table", constructor: ParseDataFrame},
		"OWNE": FrameType{id: "OWNE", description: "Ownership frame", constructor: ParseDataFrame},
		"PRIV": FrameType{id: "PRIV", description: "Private frame", constructor: ParseDataFrame},
		"PCNT": FrameType{id: "PCNT", description: "Play counter", constructor: ParseDataFrame},
		"POPM": FrameType{id: "POPM", description: "Popularimeter", constructor: ParseDataFrame},
		"POSS": FrameType{id: "POSS", description: "Position synchronisation frame", constructor: ParseDataFrame},
		"RBUF": FrameType{id: "RBUF", description: "Recommended buffer size", constructor: ParseDataFrame},
		"RVAD": FrameType{id: "RVAD", description: "Relative volume adjustment", constructor: ParseDataFrame},
		"RVRB": FrameType{id: "RVRB", description: "Reverb", constructor: ParseDataFrame},
		"SYLT": FrameType{id: "SYLT", description: "Synchronized lyric/text", constructor: ParseDataFrame},
		"SYTC": FrameType{id: "SYTC", description: "Synchronized tempo codes", constructor: ParseDataFrame},
		"TALB": FrameType{id: "TALB", description: "Album/Movie/Show title", constructor: ParseTextFrame},
		"TBPM": FrameType{id: "TBPM", description: "BPM (beats per minute)", constructor: ParseTextFrame},
		"TCOM": FrameType{id: "TCOM", description: "Composer", constructor: ParseTextFrame},
		"TCON": FrameType{id: "TCON", description: "Content type", constructor: ParseTextFrame},
		"TCOP": FrameType{id: "TCOP", description: "Copyright message", constructor: ParseTextFrame},
		"TDAT": FrameType{id: "TDAT", description: "Date", constructor: ParseTextFrame},
		"TDLY": FrameType{id: "TDLY", description: "Playlist delay", constructor: ParseTextFrame},
		"TENC": FrameType{id: "TENC", description: "Encoded by", constructor: ParseTextFrame},
		"TEXT": FrameType{id: "TEXT", description: "Lyricist/Text writer", constructor: ParseTextFrame},
		"TFLT": FrameType{id: "TFLT", description: "File type", constructor: ParseTextFrame},
		"TIME": FrameType{id: "TIME", description: "Time", constructor: ParseTextFrame},
		"TIT1": FrameType{id: "TIT1", description: "Content group description", constructor: ParseTextFrame},
		"TIT2": FrameType{id: "TIT2", description: "Title/songname/content description", constructor: ParseTextFrame},
		"TIT3": FrameType{id: "TIT3", description: "Subtitle/Description refinement", constructor: ParseTextFrame},
		"TKEY": FrameType{id: "TKEY", description: "Initial key", constructor: ParseTextFrame},
		"TLAN": FrameType{id: "TLAN", description: "Language(s)", constructor: ParseTextFrame},
		"TLEN": FrameType{id: "TLEN", description: "Length", constructor: ParseTextFrame},
		"TMED": FrameType{id: "TMED", description: "Media type", constructor: ParseTextFrame},
		"TOAL": FrameType{id: "TOAL", description: "Original album/movie/show title", constructor: ParseTextFrame},
		"TOFN": FrameType{id: "TOFN", description: "Original filename", constructor: ParseTextFrame},
		"TOLY": FrameType{id: "TOLY", description: "Original lyricist(s)/text writer(s)", constructor: ParseTextFrame},
		"TOPE": FrameType{id: "TOPE", description: "Original artist(s)/performer(s)", constructor: ParseTextFrame},
		"TORY": FrameType{id: "TORY", description: "Original release year", constructor: ParseTextFrame},
		"TOWN": FrameType{id: "TOWN", description: "File owner/licensee", constructor: ParseTextFrame},
		"TPE1": FrameType{id: "TPE1", description: "Lead performer(s)/Soloist(s)", constructor: ParseTextFrame},
		"TPE2": FrameType{id: "TPE2", description: "Band/orchestra/accompaniment", constructor: ParseTextFrame},
		"TPE3": FrameType{id: "TPE3", description: "Conductor/performer refinement", constructor: ParseTextFrame},
		"TPE4": FrameType{id: "TPE4", description: "Interpreted, remixed, or otherwise modified by", constructor: ParseTextFrame},
		"TPOS": FrameType{id: "TPOS", description: "Part of a set", constructor: ParseTextFrame},
		"TPUB": FrameType{id: "TPUB", description: "Publisher", constructor: ParseTextFrame},
		"TRCK": FrameType{id: "TRCK", description: "Track number/Position in set", constructor: ParseTextFrame},
		"TRDA": FrameType{id: "TRDA", description: "Recording dates", constructor: ParseTextFrame},
		"TRSN": FrameType{id: "TRSN", description: "Internet radio station name", constructor: ParseTextFrame},
		"TRSO": FrameType{id: "TRSO", description: "Internet radio station owner", constructor: ParseTextFrame},
		"TSIZ": FrameType{id: "TSIZ", description: "Size", constructor: ParseTextFrame},
		"TSRC": FrameType{id: "TSRC", description: "ISRC (international standard recording code)", constructor: ParseTextFrame},
		"TSSE": FrameType{id: "TSSE", description: "Software/Hardware and settings used for encoding", constructor: ParseTextFrame},
		"TYER": FrameType{id: "TYER", description: "Year", constructor: ParseTextFrame},
		"TXXX": FrameType{id: "TXXX", description: "User defined text information frame", constructor: ParseDescTextFrame},
		"UFID": FrameType{id: "UFID", description: "Unique file identifier", constructor: ParseIdFrame},
		"USER": FrameType{id: "USER", description: "Terms of use", constructor: ParseDataFrame},
		"TCMP": FrameType{id: "TCMP", description: "Part of a compilation (iTunes extension)", constructor: ParseTextFrame},
		"USLT": FrameType{id: "USLT", description: "Unsychronized lyric/text transcription", constructor: ParseUnsynchTextFrame},
		"WCOM": FrameType{id: "WCOM", description: "Commercial information", constructor: ParseDataFrame},
		"WCOP": FrameType{id: "WCOP", description: "Copyright/Legal information", constructor: ParseDataFrame},
		"WOAF": FrameType{id: "WOAF", description: "Official audio file webpage", constructor: ParseDataFrame},
		"WOAR": FrameType{id: "WOAR", description: "Official artist/performer webpage", constructor: ParseDataFrame},
		"WOAS": FrameType{id: "WOAS", description: "Official audio source webpage", constructor: ParseDataFrame},
		"WORS": FrameType{id: "WORS", description: "Official internet radio station homepage", constructor: ParseDataFrame},
		"WPAY": FrameType{id: "WPAY", description: "Payment", constructor: ParseDataFrame},
		"WPUB": FrameType{id: "WPUB", description: "Publishers official webpage", constructor: ParseDataFrame},
		"WXXX": FrameType{id: "WXXX", description: "User defined URL link frame", constructor: ParseDataFrame},
	}
)

func ParseV23Frame(reader io.Reader) Framer {
	data := make([]byte, FrameHeaderSize)
	if n, err := io.ReadFull(reader, data); n < FrameHeaderSize || err != nil {
		return nil
	}

	id := string(data[:4])
	t, ok := V23FrameTypeMap[id]
	if !ok {
		return nil
	}

	size, err := encodedbytes.NormInt(data[4:8])
	if err != nil {
		return nil
	}

	h := FrameHead{
		FrameType:   t,
		statusFlags: data[8],
		formatFlags: data[9],
		size:        size,
	}

	frameData := make([]byte, size)
	if n, err := io.ReadFull(reader, frameData); n < int(size) || err != nil {
		return nil
	}

	return t.constructor(h, frameData)
}

func V23Bytes(f Framer) []byte {
	headBytes := make([]byte, 0, FrameHeaderSize)

	headBytes = append(headBytes, f.Id()...)
	headBytes = append(headBytes, encodedbytes.NormBytes(uint32(f.Size()))...)
	headBytes = append(headBytes, f.StatusFlags(), f.FormatFlags())

	return append(headBytes, f.Bytes()...)
}
