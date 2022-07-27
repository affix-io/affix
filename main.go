// affix is a distributed dataset version control tool. Bigger than a spreadsheet,
// smaller than a database, datasets are all around us.
// Use affix to browse, download, create, fork, and publish datasets on a peer-to-peer
// network that works both on and offline.
//
// more info at: https://affix.io
package main

import (
	"os"
	"runtime/pprof"

	"github.com/affix-io/affix/cmd"
)

func main() {
	if cpuProfFilepath := os.Getenv("affix_CPU_PROFILE"); cpuProfFilepath != "" {
		f, err := os.Create(cpuProfFilepath)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	cmd.Execute()
}
