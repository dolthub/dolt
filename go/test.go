package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type WalkWork struct {
	Ref types.Ref
}

type GC struct {
	VR    	types.ValueReader
	Found 	map[hash.Hash]struct{}
	Pending []WalkWork
	Leaves  int
	VisitCB func(hash.Hash)
	Err     error
}

func NewGC(vr types.ValueReader, root types.Value) (*GC, error) {
	r, err := types.NewRef(root, vr.Format())
	if err != nil {
		return nil, err
	}
	gc := &GC{vr, make(map[hash.Hash]struct{}), []WalkWork{WalkWork{r}}, 0, nil, nil}
	return gc, nil
}

// Visit a reference seen in a chunk within the database.
// Post-condition:
// - the target hash of the reference is in gc.Found
// - if the target hash was not already in gc.Found, then
//   if the height of the reference is > 1, then work to
//   walk the target value is in gc.Pending
func (gc *GC) VisitRef(r types.Ref) error {
	_, ok := gc.Found[r.TargetHash()]
	if ok {
		return nil
	}
	if r.Height() > 1 {
		gc.Pending = append(gc.Pending, WalkWork{r})
	} else {
		gc.Leaves++
	}
	gc.Found[r.TargetHash()] = struct{}{}
	if gc.VisitCB != nil {
		gc.VisitCB(r.TargetHash())
	}
	return nil
}

func (gc *GC) Next(ctx context.Context) bool {
	if gc.Err != nil || len(gc.Pending) == 0 {
		return false
	}
	w := gc.Pending[len(gc.Pending)-1]
	gc.Pending = gc.Pending[:len(gc.Pending)-1]
	v, err := w.Ref.TargetValue(ctx, gc.VR)
	if err != nil {
		gc.Err = err
		return false
	}
	v.WalkRefs(gc.VR.Format(), gc.VisitRef)
	return len(gc.Pending) > 0
}

var Present = []string{
	"07edre19tqhet1qmkshbj8v12he2bkph",
	"0imo43pakrfiublj36tt9l8pshvhtne9",
	"0l90t5njm4redgrvm9ecvbto2d85d9mv",
	"0lmbtlq9nivtm73sajrmd0rufs7614vn",
	"0r796bf1g0qfduibrbat7dcum0cjrdd8",
	"1751jrqvouhilgl9porpapi8eko3hslg",
	"25590ipl21r517da117h1mpf085da6k5",
	"28dv1fms05lj6ee0klor7egrcrp2qb3f",
	"2gjeem87bea94f3vkbe7tr4uqlkb1c4c",
	"3827rb35s20d2vn1ia8csutm0l4fddq0",
	"3fom9af2bv85jj4b7e1h7dn9kkgpsmni",
	"3g03klkp0n67js14nclqsafe4l2uuavo",
	"3r8uah3ua0jpmpdc1qlb15tolelct3lr",
	"456u7uv33j1l78qulk13k7aqfr1kc869",
	"4ggk123qhi3ueonpror56mnmb4cbhl9m",
	"561s05hniuhijv0gh9f891b7ftheqrg4",
	"5g6vl4t9k89ia7t9ctfuptsk1gcsksul",
	"5ihegr8alc0odhe5211m0rmcfh0idjod",
	"6e58ijbcqje7kg3bcv4l23tguu904oek",
	"6h1btds02i1hddcaa5r0ntq1vpmtnitr",
	"7d0fng6ol404v2n6n78gk3vnfhjrd5br",
	"7tekhscofeirql33q9ha0nd9vaqhbnn8",
	"8v1a8dslpk6oatneo9ulk8s66o0ccor7",
	"afopqr9cscprviacrkgnmaeh17opiufg",
	"ai72fb53461jsrac45l7m9k2vijdm111",
	"bqinbml8pkceh23mpnn0daqecpq29urd",
	"chfhic2arm1vl13e1b7fub29eieor5pm",
	"d1klqjm6iraf9rg0pcno1obr0p7jqocm",
	"d802jbiabh7nfscum5d6ce4lu2o2jeh0",
	"do4c88rg06dk5l837g21ptv5cd8dvmei",
	"dsouetnumv3q26l5ji6hg4ojjhgoh139",
	"dtj5b3mshraklb840qlg8bukjusmgtg7",
	"e1gkcvbqds58d9tfj3q4svgvc9mlg9ju",
	"e2iko7igrassm61et87mihcmjqafsq0n",
	"e6tf07ujal03jsgbg5sudc2nou4ocu45",
	"ed45tduosl0s3jkhheam5s7vekvqdmlu",
	"elbv9v3eab4f9m66qjrjvqg6e49u1bq3",
	"f63g0en48lt65jml07f9nginn88eubdp",
	"fc22l7irkcklt7hmlvlr6ft253bcigp4",
	"fhk6nj5l5dkk2beo42ak0nai1ou1qinl",
	"fogkat1mbh85f9n505srl316cbq5f5b9",
	"fp55sqsbli1otqu7ofindveq4v57v2o6",
	"fpl73un7huukminshohetl3p8o1ag20u",
	"fplu3n49o9i5gjjegv5r3ouj4hsas9rr",
	"fr5ljvif562rqu8a9gsnkiq7en1dscdc",
	"gqhn9rl2fg730acllp15n0j8kclrr8ei",
	"i2ktegdd9q688f839rugfpjnemr917hp",
	"imcs7brntbi154g8umdjpu0c0gek1986",
	"j8j9i5045oggi3oi5537u66330rqvmvl",
	"jgb5vu9fga0u7e6kuej8b46p60ggoo4g",
	"jhfeiqfrtfots44galti0nqpp1r2cu1q",
	"jjdfuvic62a1rj87g13nrrpormqvadee",
	"jn362mhcse7b053jq7cfg73o5esse9uc",
	"jtmg00s9n87u3gm7d2epfmaki7r7rs0f",
	"kj33mdr9geb9eb384oo6unrq3jqqumbk",
	"kmd2q3eo0hqh01omarkf4jibcku1d99g",
	"kq7cir62m3jco4pqgn1qmdrnqfqh4un8",
	"l4gmbs2g55a9vhkic94mehhs8r77aot3",
	"lg5iv7cb8psn98c1lhk7drtq247e3anf",
	"mcelfm1cqhhsbh2pj2fg5ol7uqq1ih1m",
	"mtdsv868atcr4lhpholpb8gr2ap460ij",
	"n6rq6caikfs64m7onimsdlqcptsdihrg",
	"o7e19a3btfvh4j589o0s3huh7itc44ge",
	"p1b0huo5eq9j60q3lk28ke7bb41abagp",
	"p9cvjrsadmjpsm3o1ik06m3hgdtl0ku8",
	"plbnks5tac6ci35v5fq2rkko71771qgg",
	"ppoi819nh8531e3bro5964su3oat0b85",
	"pvkf6stf796mpnohcn21b64j1s58hj84",
	"qsgeni0oua2ma4nkqmnj857nab2sm32p",
	"sdr44fvk02v53r5b3kk39s9419l4e3tv",
	"siut7v1vji277qrspe9ha7te7h2tshko",
	"t18k7fn5j1oe34fdlul9muhk0uhp96a0",
	"urhe3anul3rpbis2o45ktu774e8hj06n",
	"v6gnkgm2c3kerb80f1lepi3j6lupe91m",
	"v8q2vj12su5a3ggl118anbg728l1bei9",
	"vop2b0kna13n8qk20n790g5bcsjpitpu",
	"vr7pms37ftnp1mit20bi4bn1s9rh2e8c",
}

const NomsDir = "/Users/aaronson/dolt_clone/bigdolt/open-images/noms"
// const NomsDir = "/Users/aaronson/dolt_clone/im-interested/.dolt/noms"

func PrintExcessFiles() {
	f, err := os.Open(NomsDir)
	if err != nil {
		panic(err.Error())
	}
	files, err := f.Readdir(2048)
	if err != nil {
		panic(err.Error())
	}
	excess := int64(0)
	for i := range files {
		fi := files[i]
		found := false
		for _, p := range Present {
			if p == fi.Name() {
				found = true
				break
			}
		}
		if !found {
			fmt.Printf("%v %v\n", fi.Name(), fi.Size())
			excess += fi.Size()
		}
	}
	fmt.Printf("Total: %v\n", excess)
}

func DoGCWalk(v types.Value, db datas.Database) *GC {
	gcStart := time.Now()
	gc, err := NewGC(db, v)
	if err != nil {
		panic(err.Error())
	}
	i := 0
	for gc.Next(context.Background()) {
		i++
		if i % 1024 == 0  {
			fmt.Printf(".")
		}
		if i % (1024 * 80) == 0 {
			fmt.Printf("\n")
		}
	}
	if i % (1024 * 80) >= 1024 {
		fmt.Printf("\n")
	}
	fmt.Printf("Chunks: %v\n", len(gc.Found))
	fmt.Printf("Leaves: %v\n", gc.Leaves)
	fmt.Printf("GC Time: %v\n", time.Now().Sub(gcStart))
	return gc
}

func main() {
	// PrintExcessFiles()
	nbf := types.Format_LD_1
	st, err := nbs.NewLocalStore(context.Background(), nbf.VersionString(), NomsDir, 256 * 1024 * 1024)
	if err != nil {
		panic(err.Error())
	}
	rh, err := st.Root(context.Background())
	if err != nil {
		panic(err.Error())
	}

	db := datas.NewDatabase(st)
	v, err := db.ReadValue(context.Background(), rh)
	if err != nil {
		panic(err.Error())
	}

	gc := DoGCWalk(v, db)

	getStart := time.Now()
	
	chks := make(chan nbs.CompressedChunk, 128)
	go func() {
		err := st.GetManyCompressed(context.Background(), hash.HashSet(gc.Found), chks)
		if err != nil {
			panic(err.Error())
		}
		close(chks)
	}()
	f, err  := os.OpenFile("/dev/null", os.O_RDWR, 0755)
	if err != nil {
		panic(err.Error())
	}
	i := 0
	for chk := range chks {
		i++
		if i % 1024 == 0  {
			fmt.Printf(".")
		}
		if i % (1024 * 80) == 0 {
			fmt.Printf("\n")
		}
		_, err := f.Write(chk.FullCompressedChunk)
		if err != nil {
			panic(err.Error())
		}
	}
	if i % (1024 * 80) >= 1024 {
		fmt.Printf("\n")
	}
	fmt.Printf("I: %v\n", i)
	fmt.Printf("Get Time: %v\n", time.Now().Sub(getStart))
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("MemStats: Sys: %v, PauseTotalNs: %v, GCPUFraction: %v\n", ms.Sys, ms.PauseTotalNs, ms.GCCPUFraction)
}

// Table Index:

// repeated { hash_prefix uint64, suffix_ordinal uint32 }
// repeated { compressed_chunk_length uint32 }
// repeated { hash_suffixes byte[12] }
// chunk_count uint32
// data_size uint64
// magic_number \xff\xb5\xd8\xc2\x24\x63\xee\x50

// chunk length includes uint32 checksum on the end

// prefix + ordinals tuples (chunk count * prefix tuple size) (be-uint64 prefix, be-uint32 ordinal)
// lengths (chunk count * length size) (be-uint32 chunk length)
// addr suffixes (chunk count * addrsuffixsize)
// chunk count (be-uint32)
// uncompressed data size (be-uint64)
// magicNumber (byte string)
