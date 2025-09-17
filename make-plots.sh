#!/usr/bin/bash
set -eu

dir=results
size=64k

latest_timestamp() {
  find $dir -maxdepth 1 -name "[0-9]*-$1-*.csv" -type f | sort | tail -1 | xargs basename 2>/dev/null | cut -c1-14
}

# Volume
ts1=$(latest_timestamp "volume-slate")
ts2=$(latest_timestamp "volume-iavl")
ts3=$(latest_timestamp "volume-doltdb")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot-xy.py \
    "$dir/$ts1-volume-slate-file.csv=Slate (file)" \
    "$dir/$ts1-volume-slate-rocksdb.csv=Slate (rocksdb)" \
    "$dir/$ts2-volume-iavl-leveldb.csv=IAVL+ (leveldb)" \
    "$dir/$ts3-volume-doltdb-file.csv=DoltDB (file)" \
    -o "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-volume.png" \
    --title "Volume Performance \$(T_{\\rm $size})\$" \
    --xlabel "Number of data" \
    --ylabel "Storage space used when all data is saved [bytes]"
  cp "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-volume.png" "bench-volume.png"
fi

# Append
ts1=$(latest_timestamp "append-slate")
ts2=$(latest_timestamp "append-iavl")
ts3=$(latest_timestamp "append-doltdb")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot-xy.py \
    "$dir/$ts1-append-slate-file.csv=Slate (file)" \
    "$dir/$ts1-append-slate-rocksdb.csv=Slate (rocksdb)" \
    "$dir/$ts1-append-slate-memkvs.csv=Slate (memkvs)" \
    "$dir/$ts1-append-seqfile-file.csv=Unindexed Sequence File" \
    "$dir/$ts2-append-iavl-leveldb.csv=IAVL+ (leveldb)" \
    "$dir/$ts3-append-doltdb-file.csv=DoltDB (file)" \
    -o "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-append.png" \
    --title "Append Performance \$(T_{\\rm $size})\$" \
    --xlabel "Number of data" \
    --ylabel "Time taken to add all data [msec]" \
    --no-latex
  cp "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-append.png" "bench-append.png"
fi

# Get
ts1=$(latest_timestamp "get-slate")
ts2=$(latest_timestamp "get-iavl")
ts3=$(latest_timestamp "get-doltdb")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot-xy.py \
    "$dir/$ts1-get-slate-file.csv=Slate (file)" \
    "$dir/$ts1-get-slate-rocksdb.csv=Slate (rocksdb)" \
    "$dir/$ts1-get-slate-memkvs.csv=Slate (memkvs)" \
    "$dir/$ts1-get-hashtree-file.csv=Binary Tree (file)" \
    "$dir/$ts2-get-iavl-leveldb.csv=IAVL+ (leveldb)" \
    "$dir/$ts3-get-doltdb-file.csv=DoltDB (file)" \
    -o "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-get.png" \
    --title "Get Performance \$(T_{\\rm $size})\$" \
    --xlabel "Distance from latest data" \
    --ylabel "Time taken to acquire data [msec]" \
    --ymin 0 \
    --xscale log \
    --xreverse \
    --no-errorbars --no-scatter
  cp "$dir/$([[ "$ts1" > "$ts2" ]] && echo "$ts1" || echo "$ts2")-get.png" "bench-get.png"
fi

# Cache
ts=$(latest_timestamp "cache0")
if [ ! -z "$ts" ]; then
  python3 scripts/scatter-plot-xy.py \
    "$dir/$ts-cache0-slate-file.csv=Level 0" \
    "$dir/$ts-cache1-slate-file.csv=Level 1" \
    "$dir/$ts-cache2-slate-file.csv=Level 2" \
    "$dir/$ts-cache3-slate-file.csv=Level 3" \
    -o "$dir/$ts-cache.png" \
    --title "Cache Performance (\$T_{\\rm $size}\$ slate file)" \
    --xlabel "Distance from latest data" \
    --ylabel "Time taken to acquire data [msec]" \
    --xscale log \
    --xreverse \
    --no-errorbars --no-scatter
  cp "$dir/$ts-cache.png" "bench-cache.png"
fi

# Prove
ts1=$(latest_timestamp "prove-slate")
if [ ! -z "$ts1" ]; then
  python3 scripts/scatter-plot-xy.py \
    "$dir/$ts1-prove-slate-file.csv=Slate (file)" \
    "$dir/$ts1-prove-slate-rocksdb.csv=Slate (rocksdb)" \
    -o "$dir/$ts1-prove.png" \
    --title "Proven Performance \$(T_{\\rm $size})\$" \
    --xlabel "Distance of differences from latest data" \
    --ylabel "Time taken to prove data [msec]" \
    --xscale log \
    --ymin 0 --ymax 0.6 \
    --no-latex --no-errorbars
  cp "$dir/$ts1-prove.png" "bench-prove.png"
fi

# Performance distribution of GET requests under skewed access patterns
ts1=$(latest_timestamp "biased-get")
if [ ! -z "$ts1" ]; then
  python3 scripts/histogram-plot.py \
    "$dir/$ts1-biased-get-slate-file_x.csv"='$s=$' \
    --output "$dir/$ts1-biased-get-slate-file_x.png" \
    --title 'Distribution of Access Positions following Zipf Bias $p(i)\propto 1/k^s$' \
    --xlabel 'Position $i$' \
    --ylabel 'Frequency $f$' \
    --bin-width 10 \
    --chart-type line
  cp "$dir/$ts1-biased-get-slate-file_x.png" "bench-biased-get-x.png"
  python3 scripts/histogram-plot.py \
    "$dir/$ts1-biased-get-slate-file_y.csv"='$s=$' \
    --output "$dir/$ts1-biased-get-slate-file_y.png" \
    --title "Distribution of Get Performance for Access with Zipf Bias (\$T_{\\rm $size}\$)" \
    --xlabel "Time [msec]" \
    --ylabel 'Frequency $f$' \
    --xmin 0.0 --xmax 0.05 \
    --bin-width 0.002 \
    --chart-type line
  cp "$dir/$ts1-biased-get-slate-file_y.png" "bench-biased-get-y.png"
fi
