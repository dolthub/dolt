package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	id3go "github.com/mikkyang/id3-go"
)

var (
	addFlag = flag.String("add", "", "Add a file to the music database")
	// TODO: Pull listing into a separate binary. There isn't anything in the music database specific to mp3.
	listFlag = flag.Bool("ls", false, "List music files")
)

func addMp3(ds *dataset.Dataset, filename string) {
	id3, err := id3go.Open(filename)
	if err != nil {
		log.Fatalf("Failed to read id3 data from %s: %s\n", filename, err)
	}
	defer id3.Close()

	mp3_file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open %s: %s\n", filename, err)
	}
	defer mp3_file.Close()

	mp3_data, err := types.NewBlob(bufio.NewReader(mp3_file))
	if err != nil {
		log.Fatalf("Failed to read mp3 data from %s: %s\n", filename, err)
	}

	new_song := SongDef{
		Title:  id3.Title(),
		Artist: id3.Artist(),
		Album:  id3.Album(),
		Year:   id3.Year(),
		Mp3:    mp3_data,
	}.New()
	songs := readSongsFromDataset(ds).Append(new_song)
	if _, ok := ds.Commit(songs.NomsValue()); ok {
		fmt.Println("Successfully committed", filename)
		printSong(new_song)
	} else {
		log.Fatalln("Failed to commit", filename)
	}
}

func listSongs(ds *dataset.Dataset) {
	songs := readSongsFromDataset(ds)
	switch num_songs := songs.Len(); num_songs {
	case 0:
		fmt.Println("No songs yet")
	case 1:
		fmt.Println("Found 1 song")
	default:
		fmt.Println("Found", num_songs, "songs")
	}
	songs.IterAll(func(song Song, i uint64) {
		fmt.Printf("(%d)\n", i)
		printSong(song)
	})
}

func readSongsFromDataset(ds *dataset.Dataset) ListOfSong {
	songs := NewListOfSong()
	if commit, ok := ds.MaybeHead(); ok {
		songs = ListOfSongFromVal(commit.Value())
	}
	return songs
}

func printSong(song Song) {
	fmt.Println("     Title:", song.Title())
	fmt.Println("    Artist:", song.Artist())
	fmt.Println("     Album:", song.Album())
	fmt.Println("      Year:", song.Year())
	fmt.Println("      Size:", song.Mp3().Len())
}

func main() {
	dsFlags := dataset.NewFlags()
	flag.Parse()

	ds := dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Close()

	if *addFlag != "" {
		addMp3(ds, *addFlag)
	}
	if *listFlag {
		listSongs(ds)
	}
}
