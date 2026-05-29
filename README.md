# matrix-line-messenger

[![Go Report Card](https://goreportcard.com/badge/github.com/highesttt/matrix-line-messenger)](https://goreportcard.com/report/github.com/highesttt/matrix-line-messenger)
![Languages](https://img.shields.io/github/languages/top/highesttt/matrix-line-messenger.svg)
[![License](https://img.shields.io/github/license/highesttt/matrix-line-messenger.svg)](LICENSE)

A Matrix bridge for LINE Messenger using mautrix-go.\
Based on the [mautrix-twilio](https://github.com/mautrix/twilio) bridge

> [!WARNING]
> When updating from a version released before February 14, 2026,
> please make sure to reset your configuration and log in again.\
> This is due to a change in the login flow that requires users to log
> in again to fetch the necessary keys for E2EE decryption.

## Known issues

> [!NOTE]
> Messages sent to the LINE Bot using Beeper Desktop may appear as
> indefinitely sending.\
> Use Beeper Mobile to send commands to the LINE Bot account after
> creating the chat with Beeper Desktop.

> [!WARNING]
> The bridge identifies itself as a LINE Chrome Extension client. LINE
> only allows one active Chrome Extension session at a time, so you
> cannot use the LINE Chrome Extension and the bridge simultaneously.
> Logging into the LINE Chrome Extension will invalidate the bridge's
> session (and vice versa). If this happens, you will need to
> re-authenticate the bridge. If you want to use LINE Chrome, you will
> need to log out of the bridge first as the bridge will try to automatically
> log back in and invalidate the LINE Chrome session again.

## Features

- [x] Messages
  - [x] Text
  - [x] Images
  - [x] Videos
  - [x] Files
  - [x] Voice notes
  - [x] Audio files
  - [x] GIFs
  - [x] Location sharing (receive only)
  - [x] Contact sharing (receive only)
    - [x] LINE friend (name only)
    - [x] Device contact (vCard)
  - [x] Mentions
- [x] Read receipts
- [x] Reaction support (Receive ONLY)
- [x] Replies
- [x] Prefetch missed chats upon starting the bridge
- [x] Group chats
- [x] Sticker retrieval support
- [x] Link previews
- [x] Message unsending/deletion
- [x] Call notifications (voice and video, with duration)
- [x] Leaving chats

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/highesttt/matrix-line-messenger.git
cd matrix-line-messenger
mkdir data
```

### 2. Choose your setup

---

## Beeper users (Docker)

### Beeper Docker prerequisites

- [Docker](https://www.docker.com/get-started/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [bbctl](https://github.com/beeper/bridge-manager) (compile from source if you don't have it):

```bash
git clone https://github.com/beeper/bridge-manager.git
cd bridge-manager
./build.sh
```

### Beeper Docker configuration

```bash
bbctl c --type bridgev2 sh-line > data/config.yaml
```

The Docker container will automatically generate a matching
`registration.yaml` from the Beeper-issued tokens in your config on
first startup.

### Beeper Docker run

```bash
docker compose up --build -d
```

---

## Self-hosted Matrix server (Docker)

### Self-hosted Docker prerequisites

- [Docker](https://www.docker.com/get-started/) and [Docker Compose](https://docs.docker.com/compose/install/)
- A Matrix homeserver (Synapse, Dendrite, Conduit, etc.)

### Self-hosted Docker configuration

Start the container once to generate the example config:

```bash
docker compose up --build
```

The container will create `data/config.yaml` and exit. Edit it and set:

- `homeserver.address` — your Matrix server URL (e.g. `http://localhost:8008`)
- `homeserver.domain` — your Matrix server domain (e.g. `your.domain.com`)
- `database.uri` — your database URI (e.g. `postgres://user:pass@host/db`)
- `bridge.permissions` — who can use the bridge

Start the container again to generate the registration file:

```bash
docker compose up --build
```

It will create `data/registration.yaml` and exit. Register the
appservice with your homeserver by adding the registration file path to
your homeserver config. For Synapse, add it to
`app_service_config_files` in `homeserver.yaml`, then restart the
homeserver.

### Self-hosted Docker run

```bash
docker compose up --build -d
```

To run without rebuilding on subsequent starts:

```bash
docker compose up -d
```

---

## Native on Windows (without Docker)

### Windows prerequisites

- [MSYS2](https://www.msys2.org/) with mingw-w64 toolchain:

```bash
winget install MSYS2.MSYS2
# Open MSYS2 MinGW 64-bit terminal and install packages:
pacman -Syu mingw-w64-x86_64-gcc cmake
```

- [Go](https://go.dev/dl/)
- [olm](https://gitlab.matrix.org/matrix-org/olm) library:

```bash
git clone https://gitlab.matrix.org/matrix-org/olm.git
cd olm
cmake -Bbuild -G "Unix Makefiles" \
  -DCMAKE_C_COMPILER=x86_64-w64-mingw32-gcc \
  -DCMAKE_CXX_COMPILER=x86_64-w64-mingw32-g++ \
  -DCMAKE_INSTALL_PREFIX=/mingw64
cmake --build build
cmake --install build
cd ..
```

- If using Beeper, compile [bbctl](https://github.com/beeper/bridge-manager) from source:

```bash
git clone https://github.com/beeper/bridge-manager.git
cd bridge-manager

# Windows only: In cmd/bbctl/run.go, remove the following lines
# because MSYS2 is still seen as Linux, but Windows doesn't support Setpgid:
#
#   if runtime.GOOS == "linux" {
#       cmd.SysProcAttr = &syscall.SysProcAttr{
#           Setpgid: true,
#       }
#   }

./build.sh
```

### Windows build

Copy the olm `.dll` and `.dll.a` files to the project root, then:

```bash
./build-windows.sh
```

### Windows configuration

**Beeper users:**

```bash
bbctl c --type bridgev2 sh-line > data/config.yaml
bbctl r sh-line > data/registration.yaml
```

**Self-hosted Matrix server:**

```bash
./matrix-line.exe -e -c data/config.yaml
# Edit data/config.yaml and set:
#   homeserver.address: http://localhost:8008  (your Matrix server URL)
#   homeserver.domain: your.domain.com         (your Matrix server domain)
#   database.uri: sqlite:///data/matrix-line.db  (or a postgres URI)
#   bridge.permissions: (who can use the bridge)

./matrix-line.exe -g -c data/config.yaml -r data/registration.yaml
# Register the appservice with your homeserver
```

### Windows run

```bash
cd data
../matrix-line.exe
```

---

## Native on Linux / macOS (without Docker)

### Linux / macOS prerequisites

- [Go](https://go.dev/dl/) and build tools (`gcc`, `make`)
- [olm](https://gitlab.matrix.org/matrix-org/olm) library (install via your package manager or build from source)
- If using Beeper, compile [bbctl](https://github.com/beeper/bridge-manager) from source

### Linux / macOS build

```bash
./build.sh
```

### Linux / macOS configuration

**Beeper users:**

```bash
bbctl c --type bridgev2 sh-line > data/config.yaml
bbctl r sh-line > data/registration.yaml
```

**Self-hosted Matrix server:**

```bash
./matrix-line -e -c data/config.yaml
# Edit data/config.yaml and set:
#   homeserver.address: http://localhost:8008  (your Matrix server URL)
#   homeserver.domain: your.domain.com         (your Matrix server domain)
#   database.uri: sqlite:///data/matrix-line.db  (or a postgres URI)
#   bridge.permissions: (who can use the bridge)

./matrix-line -g -c data/config.yaml -r data/registration.yaml
# Register the appservice with your homeserver
```

### Linux / macOS run

```bash
cd data
../matrix-line
```

---

## Login

The bridge offers two login flows:

- **QR Code** (recommended): scan the QR code with LINE on your mobile device.
  On a first login, LINE may show a PIN prompt on mobile; enter the PIN shown
  by the bridge. If a saved LINE certificate is still valid, the PIN step is
  skipped.
- **Email and Password**: enter the email/password configured on your LINE
  account. This remains available as a secondary login path.

### Via Beeper Desktop Settings

1. Open Beeper Desktop Settings
2. Navigate to `Bridges`
3. Click the three dots next to your LINE Bridge and select
   `Experimental: Add an account`
4. Follow the instructions to log in to your LINE account.

### Using the Bridge Bot (any Matrix client)

1. Open your Matrix client and start a chat with
   `@sh-linebot:your.matrix.homeserver.domain`.\
   For Beeper, use `@sh-linebot:beeper.local`.
2. Send the command `login` and follow the instructions.

## Can't log in?

There are two common reasons login can fail:

### 1. No email is set on your LINE account

The QR code flow does not require a LINE email address. The email/password
flow uses the email from your account information. If your account is older,
or you signed in with Google or Apple, you may not have an email set for your
LINE account.

**How to set an email for your LINE account:**

1. Open the LINE app on your mobile device.
2. Go to `Settings` > `Account`.
3. Tap on `Email address` and set your email address.

### 2. Letter Sealing (End-to-End Encryption)

The bridge supports both `Letter Sealing` ON and OFF accounts. Messages
to peers or groups without `Letter Sealing` are sent as plain text
automatically.

For details on how the bridge handles different Letter Sealing
combinations, see [readme/LETTER_SEALING.md](readme/LETTER_SEALING.md).
