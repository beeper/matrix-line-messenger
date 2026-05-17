package connector

import (
	"embed"
	"fmt"
)

//go:embed assets/*.png
var reactionAssets embed.FS

var predefinedReactionIcon = map[int]string{
	2: "assets/icon_1003.png",
	3: "assets/icon_1001.png",
	4: "assets/icon_1002.png",
	5: "assets/icon_1004.png",
	6: "assets/icon_1006.png",
	7: "assets/icon_1005.png",
}

func getReactionIconData(prt int) ([]byte, error) {
	path, ok := predefinedReactionIcon[prt]
	if !ok {
		return nil, fmt.Errorf("unknown predefined reaction type %d", prt)
	}
	return reactionAssets.ReadFile(path)
}
