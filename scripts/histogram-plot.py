#!/usr/bin/env python3
"""
ヒストグラムプロッター
====================================

各行が系列を示すCSVファイルからヒストグラムを描画します。各行の測定値の分布を
ヒストグラムで表示し、複数系列の比較を行うことができます。

使用方法:
    python histogram-plot.py file1.csv file2.csv=Series2 file3.csv
    python histogram-plot.py data.csv --title "測定値分布" --xlabel "時間[ms]" --bins 50

データ形式:
    SERIES_NAME,MEASUREMENTS
    0.5,0.012325,0.002397,0.004794,0.0043159999999999995,...
    1.2,0.022785,0.002948,0.005547,0.002342,...

機能:
    - 各系列（行）をヒストグラムで表示
    - 統計値（平均、標準偏差）をグラフに表示
    - 複数系列の重ね合わせまたは並列表示
    - コマンドライン引数でカスタマイズ可能
"""

import os
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import argparse
from pathlib import Path

FONT = "Neue Haas Grotesk Display Pro"


def read_csv_data_for_histogram(filepath):
    """ヒストグラム用にCSVファイルを読み込んで系列データに変換"""
    print(f"読み込み中: {filepath}")

    # CSVファイルを読み込み
    with open(filepath, "r") as f:
        lines = f.readlines()

    series_data = []

    for line in lines[1:]:  # 最初の行（ヘッダー）をスキップ
        line = line.strip()
        if not line:
            continue

        # カンマで分割
        parts = line.split(",")

        if len(parts) >= 2:
            series_name = parts[0]  # 系列名（ZIPF値など）
            
            # 2列目以降の測定値を取得
            measurements = []
            for i in range(1, len(parts)):
                if parts[i].strip():  # 空でない場合
                    try:
                        value = float(parts[i])
                        measurements.append(value)
                    except ValueError:
                        continue
            
            if measurements:  # 測定値がある場合のみ追加
                series_data.append({
                    "series_name": series_name,
                    "measurements": measurements
                })

    print(f"  系列数: {len(series_data)}")
    for series in series_data:
        print(f"    {series['series_name']}: {len(series['measurements'])}個の測定値")
    
    return series_data


def calculate_series_stats(measurements):
    """系列の統計を計算"""
    measurements = np.array(measurements)
    return {
        "count": len(measurements),
        "mean": np.mean(measurements),
        "std": np.std(measurements),
        "min": np.min(measurements),
        "max": np.max(measurements),
        "median": np.median(measurements)
    }


def parse_file_with_legend(file_arg):
    """ファイル引数を解析して (filepath, legend) を返す"""
    if "=" in file_arg:
        filepath, legend = file_arg.split("=", 1)
        return filepath.strip(), legend.strip()
    else:
        filepath = file_arg.strip()
        legend = Path(filepath).stem  # ファイル名から拡張子を除いた部分
        return filepath, legend


def get_default_title(file_args):
    """ファイル名からデフォルトのタイトルを生成"""
    if len(file_args) == 1:
        filepath, _ = parse_file_with_legend(file_args[0])
        return Path(filepath).stem
    else:
        return "Multiple Data Comparison"


def plot_histogram_multiple_files(file_args, args):
    """複数ファイルからヒストグラムをプロット"""
    if not args.no_latex:
        matplotlib.use("pgf")
        os.environ['main_memory'] = '261000000'
        os.environ['extra_mem_bot'] = '261000000'
        os.environ['font_mem_size'] = '261000000'
        plt.rcParams.update(
            {
                "text.usetex": True,
                "text.latex.preamble": r"\usepackage{fontspec}\setmainfont{Neue Haas Grotesk Display Pro}",
                "pgf.texsystem": "xelatex",
                "pgf.rcfonts": False,
            }
        )

    # 図を作成
    plt.figure(figsize=(10, 6))

    # 色のリスト
    colors = [
        "#023E8A",
        "#812D3A", 
        "#E3B935",
        "#005955",
        "#A70092",
        "blue",
        "red",
        "green",
        "orange",
        "purple",
        "brown",
        "pink",
        "gray",
    ]

    all_series_data = []
    file_legends = []

    # 全ファイルからデータを読み込み
    for i, file_arg in enumerate(file_args):
        # ファイルパスと凡例名を分離
        filepath, legend_name = parse_file_with_legend(file_arg)
        
        # ファイルの存在確認
        if not Path(filepath).exists():
            print(f"警告: ファイルが見つかりません: {filepath}")
            continue

        # データを読み込み
        series_data = read_csv_data_for_histogram(filepath)
        if not series_data:
            print(f"警告: {filepath} にデータがありません")
            continue

        all_series_data.extend(series_data)
        
        # 系列名の決定
        if "=" in file_arg:
            # filename=series_label形式の場合
            file_legends.extend([f"{legend_name} {series['series_name']}" for series in series_data])
        else:
            # デフォルトの場合はseries_nameのみ使用
            file_legends.extend([series['series_name'] for series in series_data])

    if not all_series_data:
        print("エラー: プロット可能なデータがありません")
        return

    # 全データの範囲を取得してビンを決定
    all_measurements = []
    for series in all_series_data:
        all_measurements.extend(series["measurements"])
    
    data_min, data_max = min(all_measurements), max(all_measurements)
    
    # ビンの設定
    if args.bin_width:
        # ビン幅が指定された場合
        bins = np.arange(data_min, data_max + args.bin_width, args.bin_width)
        print(f"ビン幅: {args.bin_width}, ビン数: {len(bins)-1}")
    else:
        # ビン数が指定された場合
        bins = args.bins

    # グループ化されたチャート形式でプロット
    if len(all_series_data) > 1 and args.bin_width:
        # 各ビンで系列をグループ化
        bin_centers = (bins[:-1] + bins[1:]) / 2
        
        if args.chart_type == "bar":
            bar_width = args.bin_width / (len(all_series_data) + 2)  # +2で隙間を増やす
        
        for i, series in enumerate(all_series_data):
            color = colors[i % len(colors)]
            measurements = series["measurements"]
            stats = calculate_series_stats(measurements)
            
            # ヒストグラムデータを計算
            hist_counts, _ = np.histogram(measurements, bins=bins, density=args.density)
            
            if args.chart_type == "bar":
                # バーの位置を計算
                bar_positions = bin_centers + (i - len(all_series_data)/2 + 0.5) * bar_width
                
                # バーチャートを描画
                plt.bar(
                    bar_positions,
                    hist_counts,
                    width=bar_width,
                    alpha=0.8,
                    color=color,
                    label=f"{file_legends[i]} ($\\mu$={stats['mean']:.4f}, $\\sigma$={stats['std']:.4f}, $N$={stats['count']})",
                    edgecolor='black' if args.edgecolor else None,
                    linewidth=0.5 if args.edgecolor else 0
                )
            elif args.chart_type == "line":
                # 折れ線グラフを描画
                plt.plot(
                    bin_centers,
                    hist_counts,
                    color=color,
                    marker='o',
                    markersize=4,
                    linewidth=2,
                    alpha=0.8,
                    label=f"{file_legends[i]} ($\\mu$={stats['mean']:.4f}, $\\sigma$={stats['std']:.4f}, $n$={stats['count']})"
                )
    else:
        # 従来のヒストグラム形式（bin_width未指定または単一系列）
        if args.chart_type == "line":
            # 折れ線グラフの場合
            for i, series in enumerate(all_series_data):
                color = colors[i % len(colors)]
                measurements = series["measurements"]
                stats = calculate_series_stats(measurements)
                
                # ヒストグラムデータを計算
                hist_counts, bin_edges = np.histogram(measurements, bins=bins, density=args.density)
                bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
                
                # 折れ線グラフを描画
                plt.plot(
                    bin_centers,
                    hist_counts,
                    color=color,
                    marker='o',
                    markersize=4,
                    linewidth=2,
                    alpha=0.8,
                    label=f"{file_legends[i]} $(\\mu={stats['mean']:.4f}, \\sigma={stats['std']:.4f},n={stats['count']})$"
                )
        else:
            # バーグラフの場合（従来のヒストグラム）
            alpha = 0.6 if len(all_series_data) > 1 else 0.8

            for i, series in enumerate(all_series_data):
                color = colors[i % len(colors)]
                measurements = series["measurements"]
                stats = calculate_series_stats(measurements)
                
                # ヒストグラムを描画
                n, bins_used, patches = plt.hist(
                    measurements,
                    bins=bins,
                    alpha=alpha,
                    color=color,
                    label=f"{file_legends[i]} $(\\mu={stats['mean']:.4f}, \\sigma={stats['std']:.4f}, n={stats['count']})$",
                    density=args.density,
                    edgecolor='black' if args.edgecolor else None,
                    linewidth=0.5 if args.edgecolor else 0
                )

    # 統計値を垂直線で表示
    if args.show_stats:
        for i, series in enumerate(all_series_data):
            color = colors[i % len(colors)]
            measurements = series["measurements"]
            stats = calculate_series_stats(measurements)
            
            plt.axvline(stats['mean'], color=color, linestyle='--', alpha=0.8, linewidth=1)
            if not args.no_latex:
                plt.axvline(stats['mean'] + stats['std'], color=color, linestyle=':', alpha=0.6, linewidth=1)
                plt.axvline(stats['mean'] - stats['std'], color=color, linestyle=':', alpha=0.6, linewidth=1)

    # グラフの設定
    plt.xlabel(args.xlabel, fontsize=12, fontweight="bold")
    ylabel = "Probability Density" if args.density else "Frequency"
    plt.ylabel(args.ylabel if args.ylabel != "Frequency" else ylabel, fontsize=12, fontweight="bold")
    
    # タイトルの設定
    title = args.title if args.title else get_default_title(file_args)
    if not args.no_latex:
        plt.title(f"\\textbf{{{title}}}", fontsize=14, fontweight="bold")
    else:
        plt.title(title, fontsize=14, fontweight="bold")

    # 軸の設定
    if args.xscale:
        plt.xscale(args.xscale)
    if args.yscale:
        plt.yscale(args.yscale)

    if args.xmin is not None or args.xmax is not None:
        plt.xlim(args.xmin, args.xmax)
    if args.ymin is not None or args.ymax is not None:
        plt.ylim(args.ymin, args.ymax)

    plt.legend()
    plt.grid(True, alpha=0.3)

    # 保存
    plt.savefig(args.output, dpi=300, bbox_inches="tight")
    print(f"ヒストグラムを保存しました: {args.output}")


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description="系列データヒストグラムプロッター",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  %(prog)s data1.csv data2.csv
  %(prog)s exp1.csv=実験1 exp2.csv=実験2 --title "測定値分布比較"
  %(prog)s data.csv --xlabel "時間[ms]" --bins 50 --density
  %(prog)s data.csv --bin-width 0.01 --xlabel "時間[ms]" --chart-type bar
  %(prog)s data.csv --bin-width 0.01 --chart-type line
  %(prog)s results.csv --show-stats --xmin 0 --xmax 0.1
        """,
    )

    # ファイル引数
    parser.add_argument(
        "files", nargs="+", help="CSVファイル（filename.csv=legend 形式で凡例指定可能）"
    )

    # グラフ設定
    parser.add_argument(
        "--title",
        default="",
        help="グラフタイトル (デフォルト: ファイル名のベース部分)",
    )
    parser.add_argument("--xlabel", default="Observed Value", help="X軸ラベル (デフォルト: Observed Value)")
    parser.add_argument("--ylabel", default="Frequency", help="Y軸ラベル (デフォルト: Frequency または確率密度)")
    parser.add_argument(
        "--bins", type=int, default=30, help="ヒストグラムのビン数 (デフォルト: 30)"
    )
    parser.add_argument(
        "--bin-width", type=float, help="ビンの幅を指定（--binsより優先される）"
    )
    parser.add_argument(
        "--density", action="store_true", help="確率密度で表示 (デフォルト: False)"
    )
    parser.add_argument(
        "--show-stats", action="store_true", help="統計値を垂直線で表示 (デフォルト: False)"
    )
    parser.add_argument(
        "--edgecolor", action="store_true", help="ビンの境界線を表示 (デフォルト: False)"
    )
    parser.add_argument(
        "--no-latex", action="store_true", help="LaTeXを使用しない (デフォルト: False)"
    )
    parser.add_argument(
        "--chart-type", choices=["bar", "line"], default="bar", help="チャート種類: bar (バーグラフ) または line (折れ線グラフ) (デフォルト: bar)"
    )

    # 軸範囲
    parser.add_argument("--xmin", type=float, help="X軸最小値")
    parser.add_argument("--xmax", type=float, help="X軸最大値")
    parser.add_argument("--ymin", type=float, help="Y軸最小値")
    parser.add_argument("--ymax", type=float, help="Y軸最大値")

    # スケール設定
    parser.add_argument(
        "--xscale", choices=["linear", "log"], help="X軸スケール (linear または log)"
    )
    parser.add_argument(
        "--yscale", choices=["linear", "log"], help="Y軸スケール (linear または log)"
    )

    # 出力設定
    parser.add_argument(
        "--output",
        "-o",
        default="histogram.png",
        help="出力ファイル名 (デフォルト: histogram.png)",
    )

    args = parser.parse_args()

    print(f"処理するファイル: {len(args.files)}個")
    for file_arg in args.files:
        filepath, legend = parse_file_with_legend(file_arg)
        print(f"  {filepath} → {legend}")

    # プロット実行
    plot_histogram_multiple_files(args.files, args)


if __name__ == "__main__":
    main()