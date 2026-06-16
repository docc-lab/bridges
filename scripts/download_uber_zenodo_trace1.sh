#!/usr/bin/env bash
# Download trace1 (first day) Uber sanitized traces from Zenodo artifact:
#   The Tale of Errors in Microservices (Artifact part 1) - https://zenodo.org/records/13947828
#
# trace1-sanitized.tar.zst is split into parts trace1_aa .. trace1_dn. Reassemble with:
#   cat trace1_aa trace1_ab ... > trace1-sanitized.tar.zst
# Then decompress (needs 300-500GB disk):
#   zstd -d trace1-sanitized.tar.zst
#
# Usage: ./download_uber_zenodo_trace1.sh [-o|--output DIR] [DIR]
#   -o, --output DIR   storage directory for trace1 parts and reassembled archive
#   DIR                same as --output (positional)
# Default: current directory.

set -e
OUTDIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      OUTDIR="$2"
      shift 2
      ;;
    *)
      OUTDIR="${OUTDIR:-$1}"
      shift
      ;;
  esac
done
OUTDIR="${OUTDIR:-.}"
RECORD="13947828"
BASE="https://zenodo.org/records/${RECORD}/files"

# trace1_aa through trace1_dn in order (aa..az, ba..bz, ca..cz, da..dn)
PARTS=()
for c in a b c d; do
  for s in a b c d e f g h i j k l m n o p q r s t u v w x y z; do
    [[ "$c" == "d" && "$s" > "n" ]] && break
    PARTS+=( "trace1_${c}${s}" )
  done
done

mkdir -p "$OUTDIR"
cd "$OUTDIR"

echo "Downloading ${#PARTS[@]} trace1 parts into $OUTDIR (each ~230MB; last ~165MB) ..."
for name in "${PARTS[@]}"; do
  # Zenodo URL-encodes underscore as %5F
  url_name="${name/_/%5F}"
  if [[ -f "$name" ]]; then
    echo "  skip (exists): $name"
  else
    echo "  fetching: $name"
    curl -fSL -o "$name" "${BASE}/${url_name}?download=1" || { echo "Failed: $name"; exit 1; }
  fi
done

echo "Reassembling trace1-sanitized.tar.zst ..."
for name in "${PARTS[@]}"; do
  [[ -f "$name" ]] || { echo "Missing part: $name"; exit 1; }
done
cat "${PARTS[@]}" > trace1-sanitized.tar.zst

echo "Decompress with (requires 300-500GB free): zstd -d trace1-sanitized.tar.zst"
echo "Done."
