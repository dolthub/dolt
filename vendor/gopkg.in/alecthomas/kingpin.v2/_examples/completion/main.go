package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kingpin"
)

func listHosts() []string {
	// Provide a dynamic list of hosts from a hosts file or otherwise
	// for bash completion. In this example we simply return static slice.

	// You could use this functionality to reach into a hosts file to provide
	// completion for a list of known hosts.
	return []string{"sshhost.example", "webhost.example", "ftphost.example"}
}

type NetcatCommand struct {
	hostName string
	port     int
	format   string
}

func (n *NetcatCommand) run(c *kingpin.ParseContext) error {
	fmt.Printf("Would have run netcat to hostname %v, port %d, and output format %v\n", n.hostName, n.port, n.format)
	return nil
}

func configureNetcatCommand(app *kingpin.Application) {
	c := &NetcatCommand{}
	nc := app.Command("nc", "Connect to a Host").Action(c.run)
	nc.Flag("nop-flag", "Example of a flag with no options").Bool()

	// You can provide hint options using a function to generate them
	nc.Flag("host", "Provide a hostname to nc").
		Required().
		HintAction(listHosts).
		StringVar(&c.hostName)

	// You can provide hint options statically
	nc.Flag("port", "Provide a port to connect to").
		Required().
		HintOptions("80", "443", "8080").
		IntVar(&c.port)

	// Enum/EnumVar options will be turned into completion options automatically
	nc.Flag("format", "Define the output format").
		Default("raw").
		EnumVar(&c.format, "raw", "json")

	// You can combine HintOptions with HintAction too
	nc.Flag("host-with-multi", "Define a hostname").
		HintAction(listHosts).
		HintOptions("myhost.com").
		String()

	// And combine with themselves
	nc.Flag("host-with-multi-options", "Define a hostname").
		HintOptions("myhost.com").
		HintOptions("myhost2.com").
		String()

	// If you specify HintOptions/HintActions for Enum/EnumVar, the options
	// provided for Enum/EnumVar will be overridden.
	nc.Flag("format-with-override-1", "Define a format").
		HintAction(listHosts).
		Enum("option1", "option2")

	nc.Flag("format-with-override-2", "Define a format").
		HintOptions("myhost.com", "myhost2.com").
		Enum("option1", "option2")
}

func addSubCommand(app *kingpin.Application, name string, description string) {
	c := app.Command(name, description).Action(func(c *kingpin.ParseContext) error {
		fmt.Printf("Would have run command %s.\n", name)
		return nil
	})
	c.Flag("nop-flag", "Example of a flag with no options").Bool()
}

func main() {
	app := kingpin.New("completion", "My application with bash completion.")
	app.Flag("flag-1", "").String()
	app.Flag("flag-2", "").HintOptions("opt1", "opt2").String()

	configureNetcatCommand(app)

	// Add some additional top level commands
	addSubCommand(app, "ls", "Additional top level command to show command completion")
	addSubCommand(app, "ping", "Additional top level command to show command completion")
	addSubCommand(app, "nmap", "Additional top level command to show command completion")

	kingpin.MustParse(app.Parse(os.Args[1:]))
}
