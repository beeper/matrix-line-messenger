package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/highesttt/matrix-line-messenger/pkg/connector"
)

// Information to find out exactly which commit the bridge was built from.
// These are filled at build time with the -X linker flag.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {
	m := mxmain.BridgeMain{
		Name:        "matrix-line",
		Description: "A Matrix-LINE bridge",
		URL:         "https://github.com/highesttt/matrix-line-messenger",
		Version:     "1.2.0",
		Connector:   &connector.LineConnector{},
	}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
