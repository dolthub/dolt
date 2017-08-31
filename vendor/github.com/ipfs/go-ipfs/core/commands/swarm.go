package commands

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	repo "github.com/ipfs/go-ipfs/repo"
	config "github.com/ipfs/go-ipfs/repo/config"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	iaddr "github.com/ipfs/go-ipfs/thirdparty/ipfsaddr"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	swarm "gx/ipfs/QmVJefKHXEx28RFpmj5GeRg43AqeBH3npPwvgJ875fBPm7/go-libp2p-swarm"

	mafilter "gx/ipfs/QmSMZwvs3n4GBikZ7hKzT17c3bk65FmyZo2JqtJ16swqCv/multiaddr-filter"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

type stringList struct {
	Strings []string
}

type addrMap struct {
	Addrs map[string][]string
}

var SwarmCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Interact with the swarm.",
		ShortDescription: `
'ipfs swarm' is a tool to manipulate the network swarm. The swarm is the
component that opens, listens for, and maintains connections to other
ipfs peers in the internet.
`,
	},
	Subcommands: map[string]*cmds.Command{
		"addrs":      swarmAddrsCmd,
		"connect":    swarmConnectCmd,
		"disconnect": swarmDisconnectCmd,
		"filters":    swarmFiltersCmd,
		"peers":      swarmPeersCmd,
	},
}

var swarmPeersCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List peers with open connections.",
		ShortDescription: `
'ipfs swarm peers' lists the set of peers this node is connected to.
`,
	},
	Options: []cmds.Option{
		cmds.BoolOption("verbose", "v", "display all extra information"),
		cmds.BoolOption("streams", "Also list information about open streams for each peer"),
		cmds.BoolOption("latency", "Also list information about latency to each peer"),
	},
	Run: func(req cmds.Request, res cmds.Response) {

		log.Debug("ipfs swarm peers")
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		verbose, _, _ := req.Option("verbose").Bool()
		latency, _, _ := req.Option("latency").Bool()
		streams, _, _ := req.Option("streams").Bool()

		conns := n.PeerHost.Network().Conns()

		var out connInfos
		for _, c := range conns {
			pid := c.RemotePeer()
			addr := c.RemoteMultiaddr()

			ci := connInfo{
				Addr: addr.String(),
				Peer: pid.Pretty(),
			}

			swcon, ok := c.(*swarm.Conn)
			if ok {
				ci.Muxer = fmt.Sprintf("%T", swcon.StreamConn().Conn())
			}

			if verbose || latency {
				lat := n.Peerstore.LatencyEWMA(pid)
				if lat == 0 {
					ci.Latency = "n/a"
				} else {
					ci.Latency = lat.String()
				}
			}
			if verbose || streams {
				strs, err := c.GetStreams()
				if err != nil {
					res.SetError(err, cmds.ErrNormal)
					return
				}

				for _, s := range strs {
					ci.Streams = append(ci.Streams, streamInfo{Protocol: string(s.Protocol())})
				}
			}
			sort.Sort(&ci)
			out.Peers = append(out.Peers, ci)
		}

		sort.Sort(&out)
		res.SetOutput(&out)
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			ci, ok := res.Output().(*connInfos)
			if !ok {
				return nil, fmt.Errorf("expected output type to be connInfos")
			}

			buf := new(bytes.Buffer)
			pipfs := ma.ProtocolWithCode(ma.P_IPFS).Name
			for _, info := range ci.Peers {
				ids := fmt.Sprintf("/%s/%s", pipfs, info.Peer)
				if strings.HasSuffix(info.Addr, ids) {
					fmt.Fprintf(buf, "%s", info.Addr)
				} else {
					fmt.Fprintf(buf, "%s%s", info.Addr, ids)
				}
				if info.Latency != "" {
					fmt.Fprintf(buf, " %s", info.Latency)
				}
				fmt.Fprintln(buf)

				for _, s := range info.Streams {
					if s.Protocol == "" {
						s.Protocol = "<no protocol name>"
					}

					fmt.Fprintf(buf, "  %s\n", s.Protocol)
				}
			}

			return buf, nil
		},
	},
	Type: connInfos{},
}

type streamInfo struct {
	Protocol string
}

type connInfo struct {
	Addr    string
	Peer    string
	Latency string
	Muxer   string
	Streams []streamInfo
}

func (ci *connInfo) Less(i, j int) bool {
	return ci.Streams[i].Protocol < ci.Streams[j].Protocol
}

func (ci *connInfo) Len() int {
	return len(ci.Streams)
}

func (ci *connInfo) Swap(i, j int) {
	ci.Streams[i], ci.Streams[j] = ci.Streams[j], ci.Streams[i]
}

type connInfos struct {
	Peers []connInfo
}

func (ci connInfos) Less(i, j int) bool {
	return ci.Peers[i].Addr < ci.Peers[j].Addr
}

func (ci connInfos) Len() int {
	return len(ci.Peers)
}

func (ci connInfos) Swap(i, j int) {
	ci.Peers[i], ci.Peers[j] = ci.Peers[j], ci.Peers[i]
}

var swarmAddrsCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List known addresses. Useful for debugging.",
		ShortDescription: `
'ipfs swarm addrs' lists all addresses this node is aware of.
`,
	},
	Subcommands: map[string]*cmds.Command{
		"local":  swarmAddrsLocalCmd,
		"listen": swarmAddrsListenCmd,
	},
	Run: func(req cmds.Request, res cmds.Response) {

		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		addrs := make(map[string][]string)
		ps := n.PeerHost.Network().Peerstore()
		for _, p := range ps.Peers() {
			s := p.Pretty()
			for _, a := range ps.Addrs(p) {
				addrs[s] = append(addrs[s], a.String())
			}
			sort.Sort(sort.StringSlice(addrs[s]))
		}

		res.SetOutput(&addrMap{Addrs: addrs})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			m, ok := res.Output().(*addrMap)
			if !ok {
				return nil, errors.New("failed to cast map[string]string")
			}

			// sort the ids first
			ids := make([]string, 0, len(m.Addrs))
			for p := range m.Addrs {
				ids = append(ids, p)
			}
			sort.Sort(sort.StringSlice(ids))

			buf := new(bytes.Buffer)
			for _, p := range ids {
				paddrs := m.Addrs[p]
				buf.WriteString(fmt.Sprintf("%s (%d)\n", p, len(paddrs)))
				for _, addr := range paddrs {
					buf.WriteString("\t" + addr + "\n")
				}
			}
			return buf, nil
		},
	},
	Type: addrMap{},
}

var swarmAddrsLocalCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List local addresses.",
		ShortDescription: `
'ipfs swarm addrs local' lists all local listening addresses announced to the network.
`,
	},
	Options: []cmds.Option{
		cmds.BoolOption("id", "Show peer ID in addresses.").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {

		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		showid, _, _ := req.Option("id").Bool()
		id := n.Identity.Pretty()

		var addrs []string
		for _, addr := range n.PeerHost.Addrs() {
			saddr := addr.String()
			if showid {
				saddr = path.Join(saddr, "ipfs", id)
			}
			addrs = append(addrs, saddr)
		}
		sort.Sort(sort.StringSlice(addrs))

		res.SetOutput(&stringList{addrs})
	},
	Type: stringList{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
}

var swarmAddrsListenCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List interface listening addresses.",
		ShortDescription: `
'ipfs swarm addrs listen' lists all interface addresses the node is listening on.
`,
	},
	Run: func(req cmds.Request, res cmds.Response) {

		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		var addrs []string
		maddrs, err := n.PeerHost.Network().InterfaceListenAddresses()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		for _, addr := range maddrs {
			addrs = append(addrs, addr.String())
		}
		sort.Sort(sort.StringSlice(addrs))

		res.SetOutput(&stringList{addrs})
	},
	Type: stringList{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
}

var swarmConnectCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Open connection to a given address.",
		ShortDescription: `
'ipfs swarm connect' opens a new direct connection to a peer address.

The address format is an IPFS multiaddr:

ipfs swarm connect /ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, true, "Address of peer to connect to.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		ctx := req.Context()

		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		addrs := req.Arguments()

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		snet, ok := n.PeerHost.Network().(*swarm.Network)
		if !ok {
			res.SetError(fmt.Errorf("peerhost network was not swarm"), cmds.ErrNormal)
			return
		}

		swrm := snet.Swarm()

		pis, err := peersWithAddresses(addrs)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		output := make([]string, len(pis))
		for i, pi := range pis {
			swrm.Backoff().Clear(pi.ID)

			output[i] = "connect " + pi.ID.Pretty()

			err := n.PeerHost.Connect(ctx, pi)
			if err != nil {
				res.SetError(fmt.Errorf("%s failure: %s", output[i], err), cmds.ErrNormal)
				return
			}
			output[i] += " success"
		}

		res.SetOutput(&stringList{output})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
	Type: stringList{},
}

var swarmDisconnectCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Close connection to a given address.",
		ShortDescription: `
'ipfs swarm disconnect' closes a connection to a peer address. The address
format is an IPFS multiaddr:

ipfs swarm disconnect /ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ

The disconnect is not permanent; if ipfs needs to talk to that address later,
it will reconnect.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, true, "Address of peer to disconnect from.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		addrs := req.Arguments()

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		iaddrs, err := parseAddresses(addrs)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		output := make([]string, len(iaddrs))
		for i, addr := range iaddrs {
			taddr := addr.Transport()
			output[i] = "disconnect " + addr.ID().Pretty()

			found := false
			conns := n.PeerHost.Network().ConnsToPeer(addr.ID())
			for _, conn := range conns {
				if !conn.RemoteMultiaddr().Equal(taddr) {
					log.Debug("it's not", conn.RemoteMultiaddr(), taddr)
					continue
				}

				if err := conn.Close(); err != nil {
					output[i] += " failure: " + err.Error()
				} else {
					output[i] += " success"
				}
				found = true
				break
			}

			if !found {
				output[i] += " failure: conn not found"
			}
		}
		res.SetOutput(&stringList{output})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
	Type: stringList{},
}

func stringListMarshaler(res cmds.Response) (io.Reader, error) {
	list, ok := res.Output().(*stringList)
	if !ok {
		return nil, errors.New("failed to cast []string")
	}

	buf := new(bytes.Buffer)
	for _, s := range list.Strings {
		buf.WriteString(s)
		buf.WriteString("\n")
	}
	return buf, nil
}

// parseAddresses is a function that takes in a slice of string peer addresses
// (multiaddr + peerid) and returns slices of multiaddrs and peerids.
func parseAddresses(addrs []string) (iaddrs []iaddr.IPFSAddr, err error) {
	iaddrs = make([]iaddr.IPFSAddr, len(addrs))
	for i, saddr := range addrs {
		iaddrs[i], err = iaddr.ParseString(saddr)
		if err != nil {
			return nil, cmds.ClientError("invalid peer address: " + err.Error())
		}
	}
	return
}

// peersWithAddresses is a function that takes in a slice of string peer addresses
// (multiaddr + peerid) and returns a slice of properly constructed peers
func peersWithAddresses(addrs []string) (pis []pstore.PeerInfo, err error) {
	iaddrs, err := parseAddresses(addrs)
	if err != nil {
		return nil, err
	}

	for _, iaddr := range iaddrs {
		pis = append(pis, pstore.PeerInfo{
			ID:    iaddr.ID(),
			Addrs: []ma.Multiaddr{iaddr.Transport()},
		})
	}
	return pis, nil
}

var swarmFiltersCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Manipulate address filters.",
		ShortDescription: `
'ipfs swarm filters' will list out currently applied filters. Its subcommands
can be used to add or remove said filters. Filters are specified using the
multiaddr-filter format:

Example:

    /ip4/192.168.0.0/ipcidr/16

Where the above is equivalent to the standard CIDR:

    192.168.0.0/16

Filters default to those specified under the "Swarm.AddrFilters" config key.
`,
	},
	Subcommands: map[string]*cmds.Command{
		"add": swarmFiltersAddCmd,
		"rm":  swarmFiltersRmCmd,
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrNormal)
			return
		}

		snet, ok := n.PeerHost.Network().(*swarm.Network)
		if !ok {
			res.SetError(errors.New("failed to cast network to swarm network"), cmds.ErrNormal)
			return
		}

		var output []string
		for _, f := range snet.Filters.Filters() {
			s, err := mafilter.ConvertIPNet(f)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
			output = append(output, s)
		}
		res.SetOutput(&stringList{output})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
	Type: stringList{},
}

var swarmFiltersAddCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Add an address filter.",
		ShortDescription: `
'ipfs swarm filters add' will add an address filter to the daemons swarm.
Filters applied this way will not persist daemon reboots, to achieve that,
add your filters to the ipfs config file.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, true, "Multiaddr to filter.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrNormal)
			return
		}

		snet, ok := n.PeerHost.Network().(*swarm.Network)
		if !ok {
			res.SetError(errors.New("failed to cast network to swarm network"), cmds.ErrNormal)
			return
		}

		if len(req.Arguments()) == 0 {
			res.SetError(errors.New("no filters to add"), cmds.ErrClient)
			return
		}

		r, err := fsrepo.Open(req.InvocContext().ConfigRoot)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		defer r.Close()
		cfg, err := r.Config()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		for _, arg := range req.Arguments() {
			mask, err := mafilter.NewMask(arg)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			snet.Filters.AddDialFilter(mask)
		}

		added, err := filtersAdd(r, cfg, req.Arguments())
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return

		}

		res.SetOutput(&stringList{added})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
	Type: stringList{},
}

var swarmFiltersRmCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Remove an address filter.",
		ShortDescription: `
'ipfs swarm filters rm' will remove an address filter from the daemons swarm.
Filters removed this way will not persist daemon reboots, to achieve that,
remove your filters from the ipfs config file.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("address", true, true, "Multiaddr filter to remove.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if n.PeerHost == nil {
			res.SetError(errNotOnline, cmds.ErrNormal)
			return
		}

		snet, ok := n.PeerHost.Network().(*swarm.Network)
		if !ok {
			res.SetError(errors.New("failed to cast network to swarm network"), cmds.ErrNormal)
			return
		}

		r, err := fsrepo.Open(req.InvocContext().ConfigRoot)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		defer r.Close()
		cfg, err := r.Config()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if req.Arguments()[0] == "all" || req.Arguments()[0] == "*" {
			fs := snet.Filters.Filters()
			for _, f := range fs {
				snet.Filters.Remove(f)
			}

			removed, err := filtersRemoveAll(r, cfg)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			res.SetOutput(&stringList{removed})

			return
		}

		for _, arg := range req.Arguments() {
			mask, err := mafilter.NewMask(arg)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			snet.Filters.Remove(mask)
		}

		removed, err := filtersRemove(r, cfg, req.Arguments())
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&stringList{removed})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: stringListMarshaler,
	},
	Type: stringList{},
}

func filtersAdd(r repo.Repo, cfg *config.Config, filters []string) ([]string, error) {
	addedMap := map[string]struct{}{}
	addedList := make([]string, 0, len(filters))

	// re-add cfg swarm filters to rm dupes
	oldFilters := cfg.Swarm.AddrFilters
	cfg.Swarm.AddrFilters = nil

	// add new filters
	for _, filter := range filters {
		if _, found := addedMap[filter]; found {
			continue
		}

		cfg.Swarm.AddrFilters = append(cfg.Swarm.AddrFilters, filter)
		addedList = append(addedList, filter)
		addedMap[filter] = struct{}{}
	}

	// add back original filters. in this order so that we output them.
	for _, filter := range oldFilters {
		if _, found := addedMap[filter]; found {
			continue
		}

		cfg.Swarm.AddrFilters = append(cfg.Swarm.AddrFilters, filter)
		addedMap[filter] = struct{}{}
	}

	if err := r.SetConfig(cfg); err != nil {
		return nil, err
	}

	return addedList, nil
}

func filtersRemoveAll(r repo.Repo, cfg *config.Config) ([]string, error) {
	removed := cfg.Swarm.AddrFilters
	cfg.Swarm.AddrFilters = nil

	if err := r.SetConfig(cfg); err != nil {
		return nil, err
	}

	return removed, nil
}

func filtersRemove(r repo.Repo, cfg *config.Config, toRemoveFilters []string) ([]string, error) {
	removed := make([]string, 0, len(toRemoveFilters))
	keep := make([]string, 0, len(cfg.Swarm.AddrFilters))

	oldFilters := cfg.Swarm.AddrFilters

	for _, oldFilter := range oldFilters {
		found := false
		for _, toRemoveFilter := range toRemoveFilters {
			if oldFilter == toRemoveFilter {
				found = true
				removed = append(removed, toRemoveFilter)
				break
			}
		}

		if !found {
			keep = append(keep, oldFilter)
		}
	}
	cfg.Swarm.AddrFilters = keep

	if err := r.SetConfig(cfg); err != nil {
		return nil, err
	}

	return removed, nil
}
