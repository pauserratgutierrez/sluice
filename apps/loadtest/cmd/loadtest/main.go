package main

import (
	"fmt"
	"os"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/issuer"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/jwks"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/runner"
)

func main() {
	cmd := "help"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	var err error
	switch cmd {
	case "jwks":
		err = jwks.Main()
	case "issuer":
		err = issuer.Main()
	case "run":
		err = runner.Main()
	default:
		fmt.Fprintf(os.Stderr, `loadtest — independent Sluice realtime load suite

commands:
  jwks     mint the ES256 pair if needed, then serve the public JWKS
  issuer   serve the shape-issuer used by the issuer and kick profiles
  run      run the configured LOAD_SCENARIO against a live Sluice
`)
		if cmd != "help" && cmd != "-h" && cmd != "--help" {
			os.Exit(2)
		}
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadtest %s: %v\n", cmd, err)
		os.Exit(1)
	}
}
