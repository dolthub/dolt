// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package v2

import (
	"github.com/mikkyang/id3-go/encodedbytes"
	"io"
)

const (
	V22FrameHeaderSize = 6
)

var (
	// Common frame IDs
	V22CommonFrame = map[string]FrameType{
		"Title":    V22FrameTypeMap["TT2"],
		"Artist":   V22FrameTypeMap["TP1"],
		"Album":    V22FrameTypeMap["TAL"],
		"Year":     V22FrameTypeMap["TYE"],
		"Genre":    V22FrameTypeMap["TCO"],
		"Comments": V22FrameTypeMap["COM"],
	}

	// V22FrameTypeMap specifies the frame IDs and constructors allowed in ID3v2.2
	V22FrameTypeMap = map[string]FrameType{
		"BUF": FrameType{id: "BUF", description: "Recommended buffer size", constructor: ParseDataFrame},
		"CNT": FrameType{id: "CNT", description: "Play counter", constructor: ParseDataFrame},
		"COM": FrameType{id: "COM", description: "Comments", constructor: ParseUnsynchTextFrame},
		"CRA": FrameType{id: "CRA", description: "Audio encryption", constructor: ParseDataFrame},
		"CRM": FrameType{id: "CRM", description: "Encrypted meta frame", constructor: ParseDataFrame},
		"ETC": FrameType{id: "ETC", description: "Event timing codes", constructor: ParseDataFrame},
		"EQU": FrameType{id: "EQU", description: "Equalization", constructor: ParseDataFrame},
		"GEO": FrameType{id: "GEO", description: "General encapsulated object", constructor: ParseDataFrame},
		"IPL": FrameType{id: "IPL", description: "Involved people list", constructor: ParseDataFrame},
		"LNK": FrameType{id: "LNK", description: "Linked information", constructor: ParseDataFrame},
		"MCI": FrameType{id: "MCI", description: "Music CD Identifier", constructor: ParseDataFrame},
		"MLL": FrameType{id: "MLL", description: "MPEG location lookup table", constructor: ParseDataFrame},
		"PIC": FrameType{id: "PIC", description: "Attached picture", constructor: ParseDataFrame},
		"POP": FrameType{id: "POP", description: "Popularimeter", constructor: ParseDataFrame},
		"REV": FrameType{id: "REV", description: "Reverb", constructor: ParseDataFrame},
		"RVA": FrameType{id: "RVA", description: "Relative volume adjustment", constructor: ParseDataFrame},
		"SLT": FrameType{id: "SLT", description: "Synchronized lyric/text", constructor: ParseDataFrame},
		"STC": FrameType{id: "STC", description: "Synced tempo codes", constructor: ParseDataFrame},
		"TAL": FrameType{id: "TAL", description: "Album/Movie/Show title", constructor: ParseTextFrame},
		"TBP": FrameType{id: "TBP", description: "BPM (Beats Per Minute)", constructor: ParseTextFrame},
		"TCM": FrameType{id: "TCM", description: "Composer", constructor: ParseTextFrame},
		"TCO": FrameType{id: "TCO", description: "Content type", constructor: ParseTextFrame},
		"TCR": FrameType{id: "TCR", description: "Copyright message", constructor: ParseTextFrame},
		"TDA": FrameType{id: "TDA", description: "Date", constructor: ParseTextFrame},
		"TDY": FrameType{id: "TDY", description: "Playlist delay", constructor: ParseTextFrame},
		"TEN": FrameType{id: "TEN", description: "Encoded by", constructor: ParseTextFrame},
		"TFT": FrameType{id: "TFT", description: "File type", constructor: ParseTextFrame},
		"TIM": FrameType{id: "TIM", description: "Time", constructor: ParseTextFrame},
		"TKE": FrameType{id: "TKE", description: "Initial key", constructor: ParseTextFrame},
		"TLA": FrameType{id: "TLA", description: "Language(s)", constructor: ParseTextFrame},
		"TLE": FrameType{id: "TLE", description: "Length", constructor: ParseTextFrame},
		"TMT": FrameType{id: "TMT", description: "Media type", constructor: ParseTextFrame},
		"TOA": FrameType{id: "TOA", description: "Original artist(s)/performer(s)", constructor: ParseTextFrame},
		"TOF": FrameType{id: "TOF", description: "Original filename", constructor: ParseTextFrame},
		"TOL": FrameType{id: "TOL", description: "Original Lyricist(s)/text writer(s)", constructor: ParseTextFrame},
		"TOR": FrameType{id: "TOR", description: "Original release year", constructor: ParseTextFrame},
		"TOT": FrameType{id: "TOT", description: "Original album/Movie/Show title", constructor: ParseTextFrame},
		"TP1": FrameType{id: "TP1", description: "Lead artist(s)/Lead performer(s)/Soloist(s)/Performing group", constructor: ParseTextFrame},
		"TP2": FrameType{id: "TP2", description: "Band/Orchestra/Accompaniment", constructor: ParseTextFrame},
		"TP3": FrameType{id: "TP3", description: "Conductor/Performer refinement", constructor: ParseTextFrame},
		"TP4": FrameType{id: "TP4", description: "Interpreted, remixed, or otherwise modified by", constructor: ParseTextFrame},
		"TPA": FrameType{id: "TPA", description: "Part of a set", constructor: ParseTextFrame},
		"TPB": FrameType{id: "TPB", description: "Publisher", constructor: ParseTextFrame},
		"TRC": FrameType{id: "TRC", description: "ISRC (International Standard Recording Code)", constructor: ParseTextFrame},
		"TRD": FrameType{id: "TRD", description: "Recording dates", constructor: ParseTextFrame},
		"TRK": FrameType{id: "TRK", description: "Track number/Position in set", constructor: ParseTextFrame},
		"TSI": FrameType{id: "TSI", description: "Size", constructor: ParseTextFrame},
		"TSS": FrameType{id: "TSS", description: "Software/hardware and settings used for encoding", constructor: ParseTextFrame},
		"TT1": FrameType{id: "TT1", description: "Content group description", constructor: ParseTextFrame},
		"TT2": FrameType{id: "TT2", description: "Title/Songname/Content description", constructor: ParseTextFrame},
		"TT3": FrameType{id: "TT3", description: "Subtitle/Description refinement", constructor: ParseTextFrame},
		"TXT": FrameType{id: "TXT", description: "Lyricist/text writer", constructor: ParseTextFrame},
		"TXX": FrameType{id: "TXX", description: "User defined text information frame", constructor: ParseDescTextFrame},
		"TYE": FrameType{id: "TYE", description: "Year", constructor: ParseTextFrame},
		"UFI": FrameType{id: "UFI", description: "Unique file identifier", constructor: ParseDataFrame},
		"ULT": FrameType{id: "ULT", description: "Unsychronized lyric/text transcription", constructor: ParseDataFrame},
		"WAF": FrameType{id: "WAF", description: "Official audio file webpage", constructor: ParseDataFrame},
		"WAR": FrameType{id: "WAR", description: "Official artist/performer webpage", constructor: ParseDataFrame},
		"WAS": FrameType{id: "WAS", description: "Official audio source webpage", constructor: ParseDataFrame},
		"WCM": FrameType{id: "WCM", description: "Commercial information", constructor: ParseDataFrame},
		"WCP": FrameType{id: "WCP", description: "Copyright/Legal information", constructor: ParseDataFrame},
		"WPB": FrameType{id: "WPB", description: "Publishers official webpage", constructor: ParseDataFrame},
		"WXX": FrameType{id: "WXX", description: "User defined URL link frame", constructor: ParseDataFrame},
	}
)

func ParseV22Frame(reader io.Reader) Framer {
	data := make([]byte, V22FrameHeaderSize)
	if n, err := io.ReadFull(reader, data); n < V22FrameHeaderSize || err != nil {
		return nil
	}

	id := string(data[:3])
	t, ok := V22FrameTypeMap[id]
	if !ok {
		return nil
	}

	size, err := encodedbytes.NormInt(data[3:6])
	if err != nil {
		return nil
	}

	h := FrameHead{
		FrameType: t,
		size:      size,
	}

	frameData := make([]byte, size)
	if n, err := io.ReadFull(reader, frameData); n < int(size) || err != nil {
		return nil
	}

	return t.constructor(h, frameData)
}

func V22Bytes(f Framer) []byte {
	headBytes := make([]byte, 0, V22FrameHeaderSize)

	headBytes = append(headBytes, f.Id()...)
	headBytes = append(headBytes, encodedbytes.NormBytes(uint32(f.Size()))[1:]...)

	return append(headBytes, f.Bytes()...)
}
