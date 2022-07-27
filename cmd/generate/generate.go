// Package generate is a command that creates a bash completion file for affix
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/affix-io/affix/auth/key"
	"github.com/affix-io/affix/cmd"
	"github.com/affix-io/ioes"
	"github.com/affix-io/qfs/qipfs"
	"github.com/spf13/cobra/doc"
)

func main() {
	lastArg := os.Args[len(os.Args)-1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctors := cmd.Constructors{
		CryptoGenerator: key.NewCryptoGenerator(),
		InitIPFS:        qipfs.InitRepo,
	}

	switch lastArg {
	case "completions":
		fmt.Printf("generating completions file...")
		root, _ := cmd.NewaffixCommand(ctx, cmd.StandardRepoPath(), ctors, ioes.NewStdIOStreams())
		root.GenBashCompletionFile("out.sh")
		fmt.Println("done")
	case "docs":
		fmt.Printf("generating markdown docs...")
		path := "docs"
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Fatal(err)
		}
		root, _ := cmd.NewaffixCommand(ctx, cmd.StandardRepoPath(), ctors, ioes.NewStdIOStreams())
		err := doc.GenMarkdownTree(root, path)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("done")
	default:
		fmt.Println("please provide a generate argument: [docs|completions]")
	}
}
