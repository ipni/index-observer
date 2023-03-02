package main

import (
	"fmt"
	"os"

	"github.com/ipni/index-observer/task"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name: "index-observer",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Value:   "8080",
				Usage:   "port to listen on",
			},
			&cli.StringSliceFlag{
				Name:  "indexer",
				Usage: "indexers to monitor as urls",
			},
			&cli.StringFlag{
				Name:  "source",
				Usage: "source indexer to use in progress observer",
			},
			&cli.StringFlag{
				Name:  "target",
				Usage: "target indexer to use in progress observer",
			},
		},
		Action: task.Start,
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
