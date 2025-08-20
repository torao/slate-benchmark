#!/usr/bin/bash
set -eu

dir=results

latest_timestamp() {
  find $dir -maxdepth 1 -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-$1-*.csv" -type f | sort | tail -1 | xargs basename 2>/dev/null | cut -c1-14
}

# Volume
ts1=$(latest_timestamp "volume-slate")
ts2=$(latest_timestamp "volume-iavl")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot2.py \
    "$dir/$ts1-volume-slate-file.csv=Slate (file)" \
    "$dir/$ts1-volume-slate-rocksdb.csv=Slate (rocksdb)" \
    "$dir/$ts2-volume-iavl-leveldb.csv=IAVL+ (leveldb)" \
    -o "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-volume.png" \
    --title "Volume Performance \$(T_{\\rm 1M})\$" \
    --xlabel "Number of data" \
    --ylabel "Storage space used when all data is saved [bytes]"
  cp "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-volume.png" "bench-volume.png"
fi

# Append
ts1=$(latest_timestamp "append-slate")
ts2=$(latest_timestamp "append-iavl")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot2.py \
    "$dir/$ts1-append-slate-file.csv=Slate (file)" \
    "$dir/$ts1-append-slate-rocksdb.csv=Slate (rocksdb)" \
    "$dir/$ts1-append-slate-memory.csv=Slate (memory)" \
    "$dir/$ts1-append-seqfile-file.csv=Unindexed Sequence File" \
    "$dir/$ts2-append-iavl-leveldb.csv=IAVL+ (leveldb)" \
    -o "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-append.png" \
    --title "Append Performance \$(T_{\\rm 1M})\$" \
    --xlabel "Number of data" \
    --ylabel "Time taken to add all data [msec]"
  cp "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-append.png" "bench-append.png"
fi

# Query
ts1=$(latest_timestamp "query-slate")
ts2=$(latest_timestamp "query-iavl")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot2.py \
    "$dir/$ts1-query-slate-file.csv=Slate (file)" \
    "$dir/$ts1-query-slate-rocksdb.csv=Slate (rocksdb)" \
    "$dir/$ts1-query-slate-memkvs.csv=Slate (memkvs)" \
    "$dir/$ts1-query-hashtree-file.csv=Binary Tree (file)" \
    "$dir/$ts2-query-iavl-leveldb.csv=IAVL+ (leveldb)" \
    -o "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-query.png" \
    --title "Query Performance \$(T_{\\rm 1M})\$" \
    --xlabel "Distance from latest data" \
    --ylabel "Time taken to acquire data [msec]" \
    --ymin 0 --ymax 0.2 \
    --xscale log \
    --no-errorbars
  cp "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-query.png" "bench-query.png"
fi

# Cache
ts=$(latest_timestamp "cache")
if [ ! -z "$ts" ]; then
  python3 scripts/scatter-plot2.py \
    "$dir/$ts-cache-slate-file-0.csv=Level 0" \
    "$dir/$ts-cache-slate-file-1.csv=Level 1" \
    "$dir/$ts-cache-slate-file-2.csv=Level 2" \
    "$dir/$ts-cache-slate-file-3.csv=Level 3" \
    -o "$dir/$ts-cache.png" \
    --title "Cache Performance (\$T_{\\rm 1M}\$ slate file)" \
    --xlabel "Distance from latest data" \
    --ylabel "Time taken to acquire data [msec]" \
    --xscale log \
    --no-errorbars --no-scatter
  cp "$dir/$ts-cache.png" "bench-cache.png"
fi
