package tour

import "sort"

func init() {
	for _, t := range allTopics {
		Topics[t.ID] = t
		IDs = append(IDs, t.ID)
	}

	sort.Sort(IDSlice(IDs))
}

// TODO move content into individual files if desired

// TODO(brian): If sub-topics are needed, write recursively (as tree comprised
// of Section nodes:
//
// type Section interface {
// 	Sections() []Section
// 	Topic() Topic
// }

var (
	// TODO bootstrapping

	// TODO pinning: ensuring a block is kept in local storage (i.e. not
	// evicted from cache).

	Introduction = Chapter(0)
	FileBasics   = Chapter(1)
	NodeBasics   = Chapter(2)
	MerkleDag    = Chapter(3)
	Network      = Chapter(4)
	Daemon       = Chapter(5)
	Routing      = Chapter(6)
	Exchange     = Chapter(7)
	Ipns         = Chapter(8)
	Mounting     = Chapter(9)
	Plumbing     = Chapter(10)
	Formats      = Chapter(11)
)

// Topics contains a mapping of Tour Topic ID to Topic
var allTopics = []Topic{
	{ID: Introduction(0), Content: IntroHelloMars},
	{ID: Introduction(1), Content: IntroTour},
	{ID: Introduction(2), Content: IntroAboutIpfs},

	{ID: FileBasics(1), Content: FileBasicsFilesystem},
	{ID: FileBasics(2), Content: FileBasicsGetting},
	{ID: FileBasics(3), Content: FileBasicsAdding},
	{ID: FileBasics(4), Content: FileBasicsDirectories},
	{ID: FileBasics(5), Content: FileBasicsDistributed},
	{ID: FileBasics(6), Content: FileBasicsMounting},

	{NodeBasics(0), NodeBasicsInit},
	{NodeBasics(1), NodeBasicsHelp},
	{NodeBasics(2), NodeBasicsUpdate},
	{NodeBasics(3), NodeBasicsConfig},

	{MerkleDag(0), MerkleDagIntro},
	{MerkleDag(1), MerkleDagContentAddressing},
	{MerkleDag(2), MerkleDagContentAddressingLinks},
	{MerkleDag(3), MerkleDagRedux},
	{MerkleDag(4), MerkleDagIpfsObjects},
	{MerkleDag(5), MerkleDagIpfsPaths},
	{MerkleDag(6), MerkleDagImmutability},
	{MerkleDag(7), MerkleDagUseCaseUnixFS},
	{MerkleDag(8), MerkleDagUseCaseGitObjects},
	{MerkleDag(9), MerkleDagUseCaseOperationalTransforms},

	{Network(0), Network_Intro},
	{Network(1), Network_Ipfs_Peers},
	{Network(2), Network_Daemon},
	{Network(3), Network_Routing},
	{Network(4), Network_Exchange},
	{Network(5), Network_Intro},

	// TODO daemon - {API, API Clients, Example} how old-school http + ftp
	// clients show it
	{Daemon(0), Daemon_Intro},
	{Daemon(1), Daemon_Running_Commands},
	{Daemon(2), Daemon_Web_UI},

	{Routing(0), Routing_Intro},
	{Routing(1), Rouing_Interface},
	{Routing(2), Routing_Resolving},
	{Routing(3), Routing_DHT},
	{Routing(4), Routing_Other},

	// TODO Exchange_Providing
	// TODO Exchange_Providers
	{Exchange(0), Exchange_Intro},
	{Exchange(1), Exchange_Getting_Blocks},
	{Exchange(2), Exchange_Strategies},
	{Exchange(3), Exchange_Bitswap},

	{Ipns(0), Ipns_Name_System},
	{Ipns(1), Ipns_Mutability},
	{Ipns(2), Ipns_PKI_Review},
	{Ipns(3), Ipns_Publishing},
	{Ipns(4), Ipns_Resolving},
	{Ipns(5), Ipns_Consistency},
	{Ipns(6), Ipns_Records_Etc},

	{Mounting(0), Mounting_General},
	{Mounting(1), Mounting_Ipfs},
	{Mounting(2), Mounting_Ipns},

	{Plumbing(0), Plumbing_Intro},
	{Plumbing(1), Plumbing_Ipfs_Block},
	{Plumbing(2), Plumbing_Ipfs_Object},
	{Plumbing(3), Plumbing_Ipfs_Refs},
	{Plumbing(4), Plumbing_Ipfs_Ping},
	{Plumbing(5), Plumbing_Ipfs_Id},

	{Formats(0), Formats_MerkleDag},
	{Formats(1), Formats_Multihash},
	{Formats(2), Formats_Multiaddr},
	{Formats(3), Formats_Multicodec},
	{Formats(4), Formats_Multicodec},
	{Formats(5), Formats_Multikey},
	{Formats(6), Formats_Protocol_Specific},
}

// Introduction

var IntroHelloMars = Content{
	Title: "Hello Mars",
	Text: `
	check things work
	`,
}
var IntroTour = Content{
	Title: "Hello Mars",
	Text: `
	how this works
	`,
}
var IntroAboutIpfs = Content{
	Title: "About IPFS",
}

// File Basics

var FileBasicsFilesystem = Content{
	Title: "Filesystem",
	Text: `
	`,
}
var FileBasicsGetting = Content{
	Title: "Getting Files",
	Text: `ipfs cat
	`,
}
var FileBasicsAdding = Content{
	Title: "Adding Files",
	Text: `ipfs add
	`,
}
var FileBasicsDirectories = Content{
	Title: "Directories",
	Text: `ipfs ls
	`,
}
var FileBasicsDistributed = Content{
	Title: "Distributed",
	Text: `ipfs cat from mars
	`,
}
var FileBasicsMounting = Content{
	Title: "Getting Files",
	Text: `ipfs mount (simple)
	`,
}

// Node Basics

var NodeBasicsInit = Content{
	Title: "Basics - init",

	// TODO touch on PKI
	//
	// This is somewhat relevant at 'ipfs init' since the generated key pair is the
	// basis for the node's identity in the network. A cursory nod may be
	// sufficient at that stage, and goes a long way in explaining init's raison
	// d'être.
	// NB: user is introduced to 'ipfs init' before 'ipfs add'.
	Text: `
	`,
}
var NodeBasicsHelp = Content{
	Title: "Basics - help",
	Text: `
	`,
}
var NodeBasicsUpdate = Content{
	Title: "Basics - update",
	Text: `
	`,
}
var NodeBasicsConfig = Content{
	Title: "Basics - config",
	Text: `
	`,
}

// Merkle DAG
var MerkleDagIntro = Content{}
var MerkleDagContentAddressing = Content{}
var MerkleDagContentAddressingLinks = Content{}
var MerkleDagRedux = Content{}
var MerkleDagIpfsObjects = Content{}
var MerkleDagIpfsPaths = Content{}
var MerkleDagImmutability = Content{
	Title: "Immutability",
	Text: `
	TODO plan9
	TODO git
	`,
}

var MerkleDagUseCaseUnixFS = Content{}
var MerkleDagUseCaseGitObjects = Content{}
var MerkleDagUseCaseOperationalTransforms = Content{}

var Network_Intro = Content{}
var Network_Ipfs_Peers = Content{}
var Network_Daemon = Content{}
var Network_Routing = Content{}
var Network_Exchange = Content{}
var Network_Naming = Content{}

var Daemon_Intro = Content{}
var Daemon_Running_Commands = Content{}
var Daemon_Web_UI = Content{}

var Routing_Intro = Content{}
var Rouing_Interface = Content{}
var Routing_Resolving = Content{}
var Routing_DHT = Content{}
var Routing_Other = Content{}

var Exchange_Intro = Content{}
var Exchange_Bitswap = Content{}
var Exchange_Strategies = Content{}
var Exchange_Getting_Blocks = Content{}

var Ipns_Consistency = Content{}
var Ipns_Mutability = Content{}
var Ipns_Name_System = Content{}
var Ipns_PKI_Review = Content{
	Title: "PKI Review",
	Text: `
	TODO sign verify
	`,
}
var Ipns_Publishing = Content{}
var Ipns_Records_Etc = Content{}
var Ipns_Resolving = Content{}

var Mounting_General = Content{} // TODO note fuse
var Mounting_Ipfs = Content{}    // TODO cd, ls, cat
var Mounting_Ipns = Content{}    // TODO editing

var Plumbing_Intro = Content{}
var Plumbing_Ipfs_Block = Content{}
var Plumbing_Ipfs_Object = Content{}
var Plumbing_Ipfs_Refs = Content{}
var Plumbing_Ipfs_Ping = Content{}
var Plumbing_Ipfs_Id = Content{}

var Formats_MerkleDag = Content{}
var Formats_Multihash = Content{}
var Formats_Multiaddr = Content{}
var Formats_Multicodec = Content{}
var Formats_Multikey = Content{}
var Formats_Protocol_Specific = Content{}
