package commands

import (
	"bytes"
	"fmt"
	"html/template"
	"io"

	cmds "github.com/ipfs/go-ipfs/commands"
	config "github.com/ipfs/go-ipfs/repo/config"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	tour "github.com/ipfs/go-ipfs/tour"
)

var tourCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Provide an introduction to IPFS.",
		ShortDescription: `
This is a tour that takes you through various IPFS concepts,
features, and tools to make sure you get up to speed with
IPFS very quickly. To start, run:

    ipfs tour
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("id", false, false, "The id of the topic you would like to tour."),
	},
	Subcommands: map[string]*cmds.Command{
		"list":    cmdIpfsTourList,
		"next":    cmdIpfsTourNext,
		"restart": cmdIpfsTourRestart,
	},
	Run: tourRunFunc,
}

func tourRunFunc(req cmds.Request, res cmds.Response) {

	cfg, err := req.InvocContext().GetConfig()
	if err != nil {
		res.SetError(err, cmds.ErrNormal)
		return
	}

	id := tour.TopicID(cfg.Tour.Last)
	if len(req.Arguments()) > 0 {
		id = tour.TopicID(req.Arguments()[0])
	}

	w := new(bytes.Buffer)
	t, err := tourGet(id)
	if err != nil {

		// If no topic exists for this id, we handle this error right here.
		// To help the user achieve the task, we construct a response
		// comprised of...
		// 1) a simple error message
		// 2) the full list of topics

		fmt.Fprintln(w, "ERROR")
		fmt.Fprintln(w, err)
		fmt.Fprintln(w, "")
		fprintTourList(w, tour.TopicID(cfg.Tour.Last))
		res.SetOutput(w)

		return
	}

	fprintTourShow(w, t)
	res.SetOutput(w)
}

var cmdIpfsTourNext = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Show the next IPFS Tour topic.",
	},

	Run: func(req cmds.Request, res cmds.Response) {
		w := new(bytes.Buffer)
		path := req.InvocContext().ConfigRoot
		cfg, err := req.InvocContext().GetConfig()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		id := tour.NextTopic(tour.TopicID(cfg.Tour.Last))
		topic, err := tourGet(id)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if err := fprintTourShow(w, topic); err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		// topic changed, not last. write it out.
		if string(id) != cfg.Tour.Last {
			cfg.Tour.Last = string(id)
			err := writeConfig(path, cfg)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}

		res.SetOutput(w)
	},
}

var cmdIpfsTourRestart = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Restart the IPFS Tour.",
	},

	Run: func(req cmds.Request, res cmds.Response) {
		path := req.InvocContext().ConfigRoot
		cfg, err := req.InvocContext().GetConfig()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		cfg.Tour.Last = ""
		err = writeConfig(path, cfg)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
	},
}

var cmdIpfsTourList = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Show a list of IPFS Tour topics.",
	},

	Run: func(req cmds.Request, res cmds.Response) {
		cfg, err := req.InvocContext().GetConfig()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		w := new(bytes.Buffer)
		fprintTourList(w, tour.TopicID(cfg.Tour.Last))
		res.SetOutput(w)
	},
}

func fprintTourList(w io.Writer, lastid tour.ID) {
	for _, id := range tour.IDs {
		c := ' '
		switch {
		case id == lastid:
			c = '*'
		case id.LessThan(lastid):
			c = '✓'
		}

		t := tour.Topics[id]
		fmt.Fprintf(w, "- %c %-5.5s %s\n", c, id, t.Title)
	}
}

// fprintTourShow writes a text-formatted topic to the writer
func fprintTourShow(w io.Writer, t *tour.Topic) error {
	tmpl := `
Tour {{ .ID }} - {{ .Title }}

{{ .Text }}

`
	ttempl, err := template.New("tour").Parse(tmpl)
	if err != nil {
		return err
	}
	return ttempl.Execute(w, t)
}

// tourGet returns the topic given its ID. Returns an error if topic does not
// exist.
func tourGet(id tour.ID) (*tour.Topic, error) {
	t, found := tour.Topics[id]
	if !found {
		return nil, fmt.Errorf("no topic with id: %s", id)
	}
	return &t, nil
}

// TODO share func
func writeConfig(path string, cfg *config.Config) error {
	// NB: This needs to run on the daemon.
	r, err := fsrepo.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()
	return r.SetConfig(cfg)
}
