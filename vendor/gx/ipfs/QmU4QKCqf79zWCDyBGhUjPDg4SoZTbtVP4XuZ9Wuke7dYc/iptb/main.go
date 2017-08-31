package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	util "gx/ipfs/QmU4QKCqf79zWCDyBGhUjPDg4SoZTbtVP4XuZ9Wuke7dYc/iptb/util"
	cli "gx/ipfs/QmVcLF2CgjQb5BWmYFWsDfxDjbzBfcChfdHRedxeL3dV4K/cli"
)

func parseRange(s string) ([]int, error) {
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		ranges := strings.Split(s[1:len(s)-1], ",")
		var out []int
		for _, r := range ranges {
			rng, err := expandDashRange(r)
			if err != nil {
				return nil, err
			}

			out = append(out, rng...)
		}
		return out, nil
	} else {
		i, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}

		return []int{i}, nil
	}
}

func expandDashRange(s string) ([]int, error) {
	parts := strings.Split(s, "-")
	if len(parts) == 0 {
		i, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		return []int{i}, nil
	}
	low, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}

	hi, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

	var out []int
	for i := low; i <= hi; i++ {
		out = append(out, i)
	}
	return out, nil
}

func handleErr(s string, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, s, err)
		os.Exit(1)
	}
}

func main() {
	app := cli.NewApp()
	app.Usage = "iptb is a tool for managing test clusters of ipfs nodes"
	app.Commands = []cli.Command{
		connectCmd,
		dumpStacksCmd,
		forEachCmd,
		getCmd,
		initCmd,
		killCmd,
		restartCmd,
		setCmd,
		shellCmd,
		startCmd,
		runCmd,
		logsCmd,
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var initCmd = cli.Command{
	Name:  "init",
	Usage: "create and initialize testbed nodes",
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "count, n",
			Usage: "number of ipfs nodes to initialize",
		},
		cli.IntFlag{
			Name:  "port, p",
			Usage: "port to start allocations from",
		},
		cli.BoolFlag{
			Name:  "force, f",
			Usage: "force initialization (overwrite existing configs)",
		},
		cli.BoolFlag{
			Name:  "mdns",
			Usage: "turn on mdns for nodes",
		},
		cli.StringFlag{
			Name:  "bootstrap",
			Usage: "select bootstrapping style for cluster",
			Value: "star",
		},
		cli.BoolFlag{
			Name:  "utp",
			Usage: "use utp for addresses",
		},
		cli.BoolFlag{
			Name:  "ws",
			Usage: "use websocket for addresses",
		},
		cli.StringFlag{
			Name:  "cfg",
			Usage: "override default config with values from the given file",
		},
		cli.StringFlag{
			Name:  "type",
			Usage: "select type of nodes to initialize",
		},
	},
	Action: func(c *cli.Context) error {
		if c.Int("count") == 0 {
			fmt.Printf("please specify number of nodes: '%s init -n 10'\n", os.Args[0])
			os.Exit(1)
		}
		cfg := &util.InitCfg{
			Bootstrap: c.String("bootstrap"),
			Force:     c.Bool("f"),
			Count:     c.Int("count"),
			Mdns:      c.Bool("mdns"),
			Utp:       c.Bool("utp"),
			Websocket: c.Bool("ws"),
			PortStart: c.Int("port"),
			Override:  c.String("cfg"),
			NodeType:  c.String("type"),
		}

		err := util.IpfsInit(cfg)
		handleErr("ipfs init err: ", err)
		return nil
	},
}

var startCmd = cli.Command{
	Name:  "start",
	Usage: "starts up all testbed nodes",
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "wait",
			Usage: "wait for nodes to fully come online before returning",
		},
		cli.StringFlag{
			Name:  "args",
			Usage: "extra args to pass on to the ipfs daemon",
		},
	},
	Action: func(c *cli.Context) error {
		var extra []string
		args := c.String("args")
		if len(args) > 0 {
			extra = strings.Fields(args)
		}

		if c.Args().Present() {
			nodes, err := parseRange(c.Args()[0])
			if err != nil {
				return err
			}

			for _, n := range nodes {
				nd, err := util.LoadNodeN(n)
				if err != nil {
					return fmt.Errorf("failed to load local node: %s\n", err)
				}

				err = nd.Start(extra)
				if err != nil {
					fmt.Println("failed to start node: ", err)
				}
			}
			return nil
		}

		nodes, err := util.LoadNodes()
		if err != nil {
			return err
		}
		return util.IpfsStart(nodes, c.Bool("wait"), extra)
	},
}

var killCmd = cli.Command{
	Name:    "kill",
	Usage:   "kill a given node (or all nodes if none specified)",
	Aliases: []string{"stop"},
	Action: func(c *cli.Context) error {
		if c.Args().Present() {
			nodes, err := parseRange(c.Args()[0])
			if err != nil {
				return fmt.Errorf("failed to parse node number: %s", err)
			}

			for _, n := range nodes {
				nd, err := util.LoadNodeN(n)
				if err != nil {
					return fmt.Errorf("failed to load local node: %s\n", err)
				}

				err = nd.Kill()
				if err != nil {
					fmt.Println("failed to kill node: ", err)
				}
			}
			return nil
		}
		nodes, err := util.LoadNodes()
		if err != nil {
			return err
		}

		err = util.IpfsKillAll(nodes)
		handleErr("ipfs kill err: ", err)
		return nil
	},
}

var restartCmd = cli.Command{
	Name:  "restart",
	Usage: "kill all nodes, then restart",
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "wait",
			Usage: "wait for nodes to come online before returning",
		},
	},
	Action: func(c *cli.Context) error {
		if c.Args().Present() {
			nodes, err := parseRange(c.Args()[0])
			if err != nil {
				return err
			}

			for _, n := range nodes {
				nd, err := util.LoadNodeN(n)
				if err != nil {
					return fmt.Errorf("failed to load local node: %s\n", err)
				}

				err = nd.Kill()
				if err != nil {
					fmt.Println("restart: failed to kill node: ", err)
				}

				err = nd.Start(nil)
				if err != nil {
					fmt.Println("restart: failed to start node again: ", err)
				}
			}
			return nil
		}
		nodes, err := util.LoadNodes()
		if err != nil {
			return err
		}

		err = util.IpfsKillAll(nodes)
		if err != nil {
			return fmt.Errorf("ipfs kill err: %s", err)
		}

		err = util.IpfsStart(nodes, c.Bool("wait"), nil)
		handleErr("ipfs start err: ", err)
		return nil
	},
}

var shellCmd = cli.Command{
	Name:  "shell",
	Usage: "execs your shell with certain environment variables set",
	Description: `Starts a new shell and sets some environment variables for you:

IPFS_PATH - set to testbed node 'n's IPFS_PATH
NODE[x] - set to the peer ID of node x
`,
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			fmt.Println("please specify which node you want a shell for")
			os.Exit(1)
		}
		i, err := strconv.Atoi(c.Args()[0])
		if err != nil {
			return fmt.Errorf("parse err: %s", err)
		}

		n, err := util.LoadNodeN(i)
		if err != nil {
			return err
		}

		err = n.Shell()
		handleErr("ipfs shell err: ", err)
		return nil
	},
}

var connectCmd = cli.Command{
	Name:  "connect",
	Usage: "connect two nodes together",
	Action: func(c *cli.Context) error {
		if len(c.Args()) < 2 {
			fmt.Println("iptb connect [node] [node]")
			os.Exit(1)
		}

		nodes, err := util.LoadNodes()
		if err != nil {
			return err
		}

		from, err := parseRange(c.Args()[0])
		if err != nil {
			return fmt.Errorf("failed to parse: %s", err)
		}

		to, err := parseRange(c.Args()[1])
		if err != nil {
			return fmt.Errorf("failed to parse: %s", err)
		}

		for _, f := range from {
			for _, t := range to {
				err = util.ConnectNodes(nodes[f], nodes[t])
				if err != nil {
					return fmt.Errorf("failed to connect: %s", err)
				}
			}
		}
		return nil
	},
}

var getCmd = cli.Command{
	Name:  "get",
	Usage: "get an attribute of the given node",
	Description: `Given an attribute name and a node number, prints the value of the attribute for the given node.

You can get the list of valid attributes by passing no arguments.`,
	Action: func(c *cli.Context) error {
		showUsage := func(w io.Writer) {
			fmt.Fprintln(w, "iptb get [attr] [node]")
			fmt.Fprintln(w, "Valid values of [attr] are:")
			attr_list := util.GetListOfAttr()
			for _, a := range attr_list {
				desc, err := util.GetAttrDescr(a)
				handleErr("error getting attribute description: ", err)
				fmt.Fprintf(w, "\t%s: %s\n", a, desc)
			}
		}
		switch len(c.Args()) {
		case 0:
			showUsage(os.Stdout)
		case 2:
			attr := c.Args().First()
			num, err := strconv.Atoi(c.Args()[1])
			handleErr("error parsing node number: ", err)

			ln, err := util.LoadNodeN(num)
			if err != nil {
				return err
			}

			val, err := ln.GetAttr(attr)
			handleErr("error getting attribute: ", err)
			fmt.Println(val)
		default:
			fmt.Fprintln(os.Stderr, "'iptb get' accepts exactly 0 or 2 arguments")
			showUsage(os.Stderr)
			os.Exit(1)
		}
		return nil
	},
}

var setCmd = cli.Command{
	Name:  "set",
	Usage: "set an attribute of the given node",
	Action: func(c *cli.Context) error {
		switch len(c.Args()) {
		case 3:
			attr := c.Args().First()
			val := c.Args()[1]
			nodes, err := parseRange(c.Args()[2])
			handleErr("error parsing node number: ", err)

			for _, i := range nodes {
				ln, err := util.LoadNodeN(i)
				if err != nil {
					return err
				}

				err = ln.SetAttr(attr, val)
				if err != nil {
					return fmt.Errorf("error setting attribute: %s", err)
				}
			}
		default:
			fmt.Fprintln(os.Stderr, "'iptb set' accepts exactly 2 arguments")
			os.Exit(1)
		}
		return nil
	},
}

var dumpStacksCmd = cli.Command{
	Name:  "dump-stack",
	Usage: "get a stack dump from the given daemon",
	Action: func(c *cli.Context) error {
		if len(c.Args()) < 1 {
			fmt.Println("iptb dump-stack [node]")
			os.Exit(1)
		}

		num, err := strconv.Atoi(c.Args()[0])
		handleErr("error parsing node number: ", err)

		ln, err := util.LoadNodeN(num)
		if err != nil {
			return err
		}

		addr, err := ln.APIAddr()
		if err != nil {
			return fmt.Errorf("failed to get api addr: %s", err)
		}

		resp, err := http.Get("http://" + addr + "/debug/pprof/goroutine?debug=2")
		handleErr("GET stack dump failed: ", err)
		defer resp.Body.Close()

		io.Copy(os.Stdout, resp.Body)
		return nil
	},
}

var forEachCmd = cli.Command{
	Name:            "for-each",
	Usage:           "run a given command on each node",
	SkipFlagParsing: true,
	Action: func(c *cli.Context) error {
		nodes, err := util.LoadNodes()
		if err != nil {
			return err
		}

		for _, n := range nodes {
			out, err := n.RunCmd(c.Args()...)
			if err != nil {
				return err
			}
			fmt.Print(out)
		}
		return nil
	},
}

var runCmd = cli.Command{
	Name:            "run",
	Usage:           "run a command on a given node",
	SkipFlagParsing: true,
	Action: func(c *cli.Context) error {
		n, err := strconv.Atoi(c.Args()[0])
		if err != nil {
			return err
		}

		nd, err := util.LoadNodeN(n)
		if err != nil {
			return err
		}

		out, err := nd.RunCmd(c.Args()[1:]...)
		if err != nil {
			return err
		}
		fmt.Print(out)
		return nil
	},
}

var logsCmd = cli.Command{
	Name:  "logs",
	Usage: "shows logs of given node(s), use '*' for all nodes",
	Flags: []cli.Flag{
		cli.BoolTFlag{
			Name:  "err",
			Usage: "show stderr stream",
		},
		cli.BoolTFlag{
			Name:  "out",
			Usage: "show stdout stream",
		},
		cli.BoolFlag{
			Name:  "s",
			Usage: "don't show additional info, just the log",
		},
	},
	Action: func(c *cli.Context) error {
		var nodes []util.IpfsNode
		var err error

		if c.Args()[0] == "*" {
			nodes, err = util.LoadNodes()
			if err != nil {
				return err
			}
		} else {
			for _, is := range c.Args() {
				i, err := strconv.Atoi(is)
				if err != nil {
					return err
				}
				n, err := util.LoadNodeN(i)
				if err != nil {
					return err
				}
				nodes = append(nodes, n)
			}
		}

		silent := c.Bool("s")
		stderr := c.BoolT("err")
		stdout := c.BoolT("out")

		for _, ns := range nodes {
			n, ok := ns.(*util.LocalNode)
			if !ok {
				return errors.New("logs are supported only with local nodes")
			}
			if stdout {
				if !silent {
					fmt.Printf(">>>> %s", n.Dir)
					fmt.Println("/daemon.stdout")
				}
				st, err := n.StderrReader()
				if err != nil {
					return err
				}
				io.Copy(os.Stdout, st)
				st.Close()
				if !silent {
					fmt.Println("<<<<")
				}
			}
			if stderr {
				if !silent {
					fmt.Printf(">>>> %s", n.Dir)
					fmt.Println("/daemon.stderr")
				}
				st, err := n.StderrReader()
				if err != nil {
					return err
				}
				io.Copy(os.Stdout, st)
				st.Close()
				if !silent {
					fmt.Println("<<<<")
				}
			}
		}

		return nil
	},
}
