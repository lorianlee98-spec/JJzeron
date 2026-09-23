# JJzeron packaging

The Cargo package is still named `zeron`; its executable is `jjzeron`.

## Linux

Run `scripts/package-linux.sh` (or `PROFILE=debug scripts/package-linux.sh` for a smoke package). It writes `target/package/jjzeron-<version>-linux-<arch>.tar.gz` with the `jjzeron` binary, `jjzeron.desktop`, `jjzeron.png`, and a local `install.sh` for `~/.local`.

## macOS

Run `scripts/package-macos.sh`. It writes `jjzeron-<version>-macos-<arch>.dmg` and `jjzeron-<version>-macos-<arch>-app.tar.gz`, both containing `JJzeron.app` with bundle ID `sh.jjzeron.app` and executable `jjzeron`. Set `CODESIGN_IDENTITY` and the notarization variables described in the script for a distributable signed build. The app checks this fork's GitHub Releases for updates by default, including in local mode.

## Windows

Run `scripts/package-windows.ps1 -ReleasesUrl https://github.com/lorianlee98-spec/JJzeron/releases/latest/download`. It packages `jjzeron.exe` with `jjzeron-update.json`. The fork never downloads original Zeron releases by default.
