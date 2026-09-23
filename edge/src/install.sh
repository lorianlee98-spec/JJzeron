#!/bin/sh
# JJzeron (native) headless installer.
#
#   ZERON_RELEASES_URL=https://your-host/releases sh install.sh
#
# Installs the self-contained native binary (no runtime deps) to
# ~/.jjzeron/app, puts `jjzeron` on PATH, and runs it as a local-only
# systemd user service that survives reboots. Signing in is optional and
# enables sync after a restart. Re-running
# upgrades in place; ~/.jjzeron state is preserved.
#
# The update feed is supplied explicitly. Sync endpoint overrides (if any)
# go in ~/.jjzeron/env.
set -eu

BASE="${ZERON_RELEASES_URL:-}"
case "$BASE" in
  https://*) BASE="${BASE%/}" ;;
  *) echo "JJzeron install: set ZERON_RELEASES_URL to an HTTPS release feed" >&2; exit 1 ;;
esac

# --- platform ---------------------------------------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux) plat=linux ;;
  Darwin)
    echo "JJzeron install: on macOS, download the desktop app instead:" >&2
    echo "  $BASE/latest.txt → $BASE/jjzeron-<version>-macos-arm64.dmg" >&2
    exit 1
    ;;
  *)
    echo "JJzeron install: unsupported OS '$os' — only Linux for now." >&2
    exit 1
    ;;
esac
case "$arch" in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *)
    echo "JJzeron install: unsupported architecture '$arch'." >&2
    exit 1
    ;;
esac

# --- download ----------------------------------------------------------------
ver="$(curl -fsSL "$BASE/latest.txt" | tr -d '[:space:]')"
[ -n "$ver" ] || { echo "JJzeron install: could not resolve latest version" >&2; exit 1; }
case "$ver" in
  *[!0-9.]* | .* | *..* | *.)
    echo "JJzeron install: invalid release version '$ver'" >&2
    exit 1
    ;;
esac
file="jjzeron-$ver-$plat-$arch.tar.gz"
data_root="$HOME/.jjzeron"
app_root="$data_root/app"
dest="$app_root/$ver"

if [ -x "$dest/jjzeron" ]; then
  echo "JJzeron $ver already downloaded — relinking."
else
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  echo "downloading JJzeron $ver ($plat-$arch)…"
  curl -fSL --progress-bar "$BASE/$file" -o "$tmp/$file"
  mkdir -p "$dest"
  tar -xzf "$tmp/$file" -C "$dest" --strip-components=1
fi

ln -sfn "$dest" "$app_root/current"
mkdir -p "$HOME/.local/bin"
ln -sf "$app_root/current/jjzeron" "$HOME/.local/bin/jjzeron"

# --- service -----------------------------------------------------------------
# The daemon is useful before auth: without a saved session it serves the local
# profile. Login only changes which profile the next daemon start selects.

service=manual
if command -v systemctl >/dev/null 2>&1 && [ -n "${XDG_RUNTIME_DIR:-}" ]; then
  mkdir -p "$HOME/.config/systemd/user"
  cat >"$HOME/.config/systemd/user/jjzeron.service" <<'UNIT'
[Unit]
Description=JJzeron native headless engine
After=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
ExecStart=%h/.jjzeron/app/current/jjzeron headless
Restart=on-failure
RestartSec=5
EnvironmentFile=-%h/.jjzeron/env

[Install]
WantedBy=default.target
UNIT
  systemctl --user daemon-reload
  systemctl --user enable jjzeron
  systemctl --user restart jjzeron
  service=running
  # Keep the user manager (and the engine) running without an active login.
  loginctl enable-linger "$USER" 2>/dev/null \
    || sudo -n loginctl enable-linger "$USER" 2>/dev/null \
    || echo "warn: could not enable linger — the engine stops when you log out (run: sudo loginctl enable-linger $USER)"
else
  echo "warn: systemd user session not available — run the engine manually with: jjzeron headless"
fi

# --- agent CLIs ---------------------------------------------------------------
command -v claude >/dev/null 2>&1 || \
  echo "note: Claude Code CLI not found — install it with: curl -fsSL https://claude.ai/install.sh | bash"

case ":$PATH:" in
  *":$HOME/.local/bin:"*) path_hint="" ;;
  *) path_hint=' (add ~/.local/bin to your PATH)' ;;
esac

echo ""
echo "✓ JJzeron $ver installed$path_hint"
echo ""
case "$service" in
  running)
    echo "the engine is running with the new version (local-only unless sync is enabled)."
    echo "  systemctl --user status jjzeron    check the service"
    echo ""
    echo "optional sync (local sessions stay local):"
    echo "  systemctl --user stop jjzeron"
    echo "  jjzeron login"
    echo "  systemctl --user restart jjzeron"
    ;;
  manual)
    echo "next: run the local-only engine with \`jjzeron headless\`."
    echo "optional sync: run \`jjzeron login\` before starting the engine."
    ;;
esac
