#!/bin/bash
# Build script for Windows


echo "Building matrix-line for Windows..."
MAUTRIX_VERSION=$(cat go.mod | grep 'maunium.net/go/mautrix ' | awk '{ print $2 }' | head -n1)
GO_LDFLAGS="-s -w -X main.Tag=$(git describe --exact-match --tags 2>/dev/null) -X main.Commit=$(git rev-parse HEAD) -X 'main.BuildTime=`date -Iseconds`' -X 'maunium.net/go/mautrix.GoModVersion=$MAUTRIX_VERSION'"
CGO_LDFLAGS='-L .' CC=x86_64-w64-mingw32-gcc go build -ldflags="$GO_LDFLAGS" -o matrix-line.exe ./cmd/matrix-line "$@"

if [ $? -eq 0 ]; then
    echo ""
    echo "Build successful: matrix-line.exe"
    echo ""
    echo "To run the bridge:"
    echo "  cd data"
    echo "  ../matrix-line.exe"
else
    echo "Build failed"
    exit 1
fi
