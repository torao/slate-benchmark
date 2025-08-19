#!/usr/bin/bash
set -eu

# pip install matplotlib pandas

# cargo run --release

# cd iavl && mise run build && cd ..

latest_timestamp() {
  find . -maxdepth 1 -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-$1-*.csv" -type f | sort | tail -1 | xargs basename 2>/dev/null | cut -c1-14
}

# Append
ts1=$(latest_timestamp "append-slate")
ts2=$(latest_timestamp "append-iavl")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot2.py \
    "$ts1-append-slate-file.csv=Slate (file)" \
    "$ts1-append-slate-rocksdb.csv=Slate (rocksdb)" \
    "$ts1-append-slate-memory.csv=Slate (memory)" \
    "$ts1-append-seqfile-file.csv=Unindexed Sequence File" \
    "$ts2-append-iavl-leveldb_time.csv=IAVL+ (leveldb)" \
    -o "$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-append-time.png" \
    --title "Append Performance \$(T_{\\rm 1M})\$" \
    --xlabel "Number of data" \
    --ylabel "Time taken to add all data [msec]"
  python3 scripts/scatter-plot2.py \
    "$ts2-append-iavl-leveldb_space.csv=IAVL+ (leveldb)" \
    -o "$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-append-space.png" \
    --title "Append Performance \$(T_{\\rm 1M})\$" \
    --xlabel "Number of data" \
    --ylabel "Storage space used when all data is saved [bytes]"
fi

# Query
ts1=$(latest_timestamp "query-slate")
ts2=$(latest_timestamp "query-iavl")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot2.py \
    "$ts1-query-slate-file.csv=Slate (file)" \
    "$ts1-query-slate-rocksdb.csv=Slate (rocksdb)" \
    "$ts1-query-slate-memkvs.csv=Slate (memkvs)" \
    "$ts1-query-hashtree-file.csv=Binary Tree (file)" \
    "$ts2-query-iavl-leveldb.csv=IAVL+ (leveldb)" \
    -o "$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-query.png" \
    --title "Query Performance \$(T_{\\rm 1M})\$" \
    --xlabel "Distance from latest data" \
    --ylabel "Time taken to acquire data [msec]" \
    --ymin 0 --ymax 0.2 \
    --xscale log \
    --no-errorbars
fi

# Cache
ts=$(latest_timestamp "cache")
if [ ! -z "$ts" ]; then
  python3 scripts/scatter-plot2.py \
    "$ts-cache-slate-file-0.csv=Level 0" \
    "$ts-cache-slate-file-1.csv=Level 1" \
    "$ts-cache-slate-file-2.csv=Level 2" \
    "$ts-cache-slate-file-3.csv=Level 3" \
    -o "$ts-cache.png" \
    --title "Cache Performance (\$T_{\\rm 1M}\$ slate file)" \
    --xlabel "Distance from latest data" \
    --ylabel "Time taken to acquire data [msec]" \
    --xscale log \
    --no-errorbars --no-scatter
fi

# python3 scripts/scatter-plot.py \
#   $(find . -maxdepth 1 -name '*-query-slate-file-large.csv' -type f 2>/dev/null | sort -r | head -n 1) \
#   --ymax 0.3 --ymin 0 \
#   --xscale log \
#   --line-width 1.2 --errorbar-width 0 --marker-size 4 \
#   --stats-position top-left --legend-position top-right \
#   --title 'Access time relative to the distance from the latest entry $(T_{128M})$' \
#   --xlabel 'distance from the latest entry $(n-i+1)$' \
#   --ylabel 'data acquation time [msec]'

# python3 scripts/scatter-plot.py \
#   $(find . -maxdepth 1 -name '*-query-slate-file-small.csv' -type f 2>/dev/null | sort -r | head -n 1) \
#   --ymax 0.1 --ymin 0 \
#   --xscale linear \
#   --line-width 1.2 --errorbar-width 0 --marker-size 4 \
#   --stats-position top-left --legend-position top-right \
#   --title 'Access time relative to the distance from the latest entry $(T_{512})$' \
#   --xlabel 'distance from the latest entry $(n-i+1)$' \
#   --ylabel 'data acquation time [msec]'

# python3 scripts/scatter-plot.py \
#   $(find . -maxdepth 1 -name '*-volume-slate-file.csv' -type f 2>/dev/null | sort -r | head -n 1) \
#   --line-width 1.2 --errorbar-width 0 --marker-size 4 \
#   --stats-position top-left --legend-position top-right \
#   --xlabel 'number of entries' \
#   --ylabel 'file size [B]'
