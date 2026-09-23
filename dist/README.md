# JJzeron packaging

The Cargo package is still named `zeron`; its executable is `jjzeron`.

## Linux

Run `scripts/package-linux.sh` (or `PROFILE=debug scripts/package-linux.sh` for a smoke package). It writes `target/package/jjzeron-<version>-linux-<arch>.tar.gz` with the `jjzeron` binary, `jjzeron.desktop`, `jjzeron.png`, and a local `install.sh` for `~/.local`.

## macOS

Run `scripts/package-macos.sh`. It writes `jjzeron-<version>-macos-<arch>.dmg` and `jjzeron-<version>-macos-<arch>-app.tar.gz`, both containing `JJzeron.app` with bundle ID `sh.jjzeron.app` and executable `jjzeron`. Set `CODESIGN_IDENTITY` and the notarization variables described in the script for a distributable signed build.

## Windows

Run `scripts/package-windows.ps1 -ReleasesUrl <your fork release feed>`. It packages `jjzeron.exe` with `jjzeron-update.json`. The update feed must be explicit; the fork never downloads original Zeron releases by default.
