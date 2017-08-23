package commands

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	cmds "github.com/ipfs/go-ipfs/commands"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

var KeyCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Create and list IPNS name keypairs",
		ShortDescription: `
'ipfs key gen' generates a new keypair for usage with IPNS and 'ipfs name publish'.

  > ipfs key gen --type=rsa --size=2048 mykey
  > ipfs name publish --key=mykey QmSomeHash

'ipfs key list' lists the available keys.

  > ipfs key list
  self
  mykey
		`,
	},
	Subcommands: map[string]*cmds.Command{
		"gen":    keyGenCmd,
		"list":   keyListCmd,
		"rename": keyRenameCmd,
		"rm":     keyRmCmd,
	},
}

type KeyOutput struct {
	Name string
	Id   string
}

type KeyOutputList struct {
	Keys []KeyOutput
}

// KeyRenameOutput define the output type of keyRenameCmd
type KeyRenameOutput struct {
	Was       string
	Now       string
	Id        string
	Overwrite bool
}

var keyGenCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Create a new keypair",
	},
	Options: []cmds.Option{
		cmds.StringOption("type", "t", "type of the key to create [rsa, ed25519]"),
		cmds.IntOption("size", "s", "size of the key to generate"),
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("name", true, false, "name of key to create"),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		typ, f, err := req.Option("type").String()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if !f {
			res.SetError(fmt.Errorf("please specify a key type with --type"), cmds.ErrNormal)
			return
		}

		size, sizefound, err := req.Option("size").Int()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		name := req.Arguments()[0]
		if name == "self" {
			res.SetError(fmt.Errorf("cannot create key with name 'self'"), cmds.ErrNormal)
			return
		}

		var sk ci.PrivKey
		var pk ci.PubKey

		switch typ {
		case "rsa":
			if !sizefound {
				res.SetError(fmt.Errorf("please specify a key size with --size"), cmds.ErrNormal)
				return
			}

			priv, pub, err := ci.GenerateKeyPairWithReader(ci.RSA, size, rand.Reader)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			sk = priv
			pk = pub
		case "ed25519":
			priv, pub, err := ci.GenerateEd25519Key(rand.Reader)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			sk = priv
			pk = pub
		default:
			res.SetError(fmt.Errorf("unrecognized key type: %s", typ), cmds.ErrNormal)
			return
		}

		err = n.Repo.Keystore().Put(name, sk)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		pid, err := peer.IDFromPublicKey(pk)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&KeyOutput{
			Name: name,
			Id:   pid.Pretty(),
		})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			k, ok := res.Output().(*KeyOutput)
			if !ok {
				return nil, fmt.Errorf("expected a KeyOutput as command result")
			}

			return strings.NewReader(k.Id + "\n"), nil
		},
	},
	Type: KeyOutput{},
}

var keyListCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List all local keypairs",
	},
	Options: []cmds.Option{
		cmds.BoolOption("l", "Show extra information about keys."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		keys, err := n.Repo.Keystore().List()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		sort.Strings(keys)

		list := make([]KeyOutput, 0, len(keys)+1)

		list = append(list, KeyOutput{Name: "self", Id: n.Identity.Pretty()})

		for _, key := range keys {
			privKey, err := n.Repo.Keystore().Get(key)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			pubKey := privKey.GetPublic()

			pid, err := peer.IDFromPublicKey(pubKey)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			list = append(list, KeyOutput{Name: key, Id: pid.Pretty()})
		}

		res.SetOutput(&KeyOutputList{list})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: keyOutputListMarshaler,
	},
	Type: KeyOutputList{},
}

var keyRenameCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Rename a keypair",
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("name", true, false, "name of key to rename"),
		cmds.StringArg("newName", true, false, "new name of the key"),
	},
	Options: []cmds.Option{
		cmds.BoolOption("force", "f", "Allow to overwrite an existing key."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		ks := n.Repo.Keystore()

		name := req.Arguments()[0]
		newName := req.Arguments()[1]

		if name == "self" {
			res.SetError(fmt.Errorf("cannot rename key with name 'self'"), cmds.ErrNormal)
			return
		}

		if newName == "self" {
			res.SetError(fmt.Errorf("cannot overwrite key with name 'self'"), cmds.ErrNormal)
			return
		}

		oldKey, err := ks.Get(name)
		if err != nil {
			res.SetError(fmt.Errorf("no key named %s was found", name), cmds.ErrNormal)
			return
		}

		pubKey := oldKey.GetPublic()

		pid, err := peer.IDFromPublicKey(pubKey)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		overwrite := false
		force, _, _ := res.Request().Option("f").Bool()
		if force {
			exist, err := ks.Has(newName)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			if exist {
				overwrite = true
				err := ks.Delete(newName)
				if err != nil {
					res.SetError(err, cmds.ErrNormal)
					return
				}
			}
		}

		err = ks.Put(newName, oldKey)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = ks.Delete(name)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&KeyRenameOutput{
			Was:       name,
			Now:       newName,
			Id:        pid.Pretty(),
			Overwrite: overwrite,
		})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			k, ok := res.Output().(*KeyRenameOutput)
			if !ok {
				return nil, fmt.Errorf("expected a KeyRenameOutput as command result")
			}

			buf := new(bytes.Buffer)

			if k.Overwrite {
				fmt.Fprintf(buf, "Key %s renamed to %s with overwriting\n", k.Id, k.Now)
			} else {
				fmt.Fprintf(buf, "Key %s renamed to %s\n", k.Id, k.Now)
			}
			return buf, nil
		},
	},
	Type: KeyRenameOutput{},
}

var keyRmCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Remove a keypair",
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("name", true, true, "names of keys to remove").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.BoolOption("l", "Show extra information about keys."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		names := req.Arguments()

		list := make([]KeyOutput, 0, len(names))
		for _, name := range names {
			if name == "self" {
				res.SetError(fmt.Errorf("cannot remove key with name 'self'"), cmds.ErrNormal)
				return
			}

			removed, err := n.Repo.Keystore().Get(name)
			if err != nil {
				res.SetError(fmt.Errorf("no key named %s was found", name), cmds.ErrNormal)
				return
			}

			pubKey := removed.GetPublic()

			pid, err := peer.IDFromPublicKey(pubKey)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			list = append(list, KeyOutput{Name: name, Id: pid.Pretty()})
		}

		for _, name := range names {
			err = n.Repo.Keystore().Delete(name)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}

		res.SetOutput(&KeyOutputList{list})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: keyOutputListMarshaler,
	},
	Type: KeyOutputList{},
}

func keyOutputListMarshaler(res cmds.Response) (io.Reader, error) {
	withId, _, _ := res.Request().Option("l").Bool()

	list, ok := res.Output().(*KeyOutputList)
	if !ok {
		return nil, errors.New("failed to cast []KeyOutput")
	}

	buf := new(bytes.Buffer)
	w := tabwriter.NewWriter(buf, 1, 2, 1, ' ', 0)
	for _, s := range list.Keys {
		if withId {
			fmt.Fprintf(w, "%s\t%s\t\n", s.Id, s.Name)
		} else {
			fmt.Fprintf(w, "%s\n", s.Name)
		}
	}
	w.Flush()
	return buf, nil
}
