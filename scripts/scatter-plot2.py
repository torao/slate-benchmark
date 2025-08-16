#!/usr/bin/env python3
"""
シンプルな複数CSV実験データプロッター
====================================

使用方法:
    python plot.py file1.csv file2.csv=Series2 file3.csv
    python plot.py data.csv --title "実験結果" --xlabel "サンプル数" --yscale log

データ形式:
    N,TIME
    1,0.204,0.1191,0.1097
    116509,1382.9469,1362.317
    
機能:
    - 各ファイルを1系列としてプロット
    - 全ての測定値を散布図で表示
    - 平均値をエラーバー(±2σ)付き折れ線で表示
    - コマンドライン引数でカスタマイズ可能
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import argparse
from pathlib import Path

FONT = "Neue Haas Grotesk Text Pro"


def read_csv_data(filepath):
    """CSVファイルを読み込んで測定データに変換"""
    print(f"読み込み中: {filepath}")
    
    # CSVファイルを読み込み
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    # ヘッダーをスキップして、データ行を処理
    data_points = []
    
    for line in lines[1:]:  # 最初の行（ヘッダー）をスキップ
        line = line.strip()
        if not line:
            continue
            
        # カンマで分割
        parts = line.split(',')
        
        if len(parts) >= 2:
            x_value = float(parts[0])  # N値
            
            # 2列目以降の測定値を取得
            for i in range(1, len(parts)):
                if parts[i].strip():  # 空でない場合
                    try:
                        y_value = float(parts[i])
                        data_points.append({'X': x_value, 'Y': y_value})
                    except ValueError:
                        continue
    
    df = pd.DataFrame(data_points)
    print(f"  データ点数: {len(df)}")
    return df

def calculate_stats(df):
    """X値ごとの統計を計算"""
    stats = df.groupby('X')['Y'].agg(['count', 'mean', 'std']).reset_index()
    stats['std'] = stats['std'].fillna(0)  # NaNを0に変換
    stats['error'] = 2 * stats['std']  # 2σエラーバー
    return stats

def parse_file_with_legend(file_arg):
    """ファイル引数を解析して (filepath, legend) を返す"""
    if '=' in file_arg:
        filepath, legend = file_arg.split('=', 1)
        return filepath.strip(), legend.strip()
    else:
        filepath = file_arg.strip()
        legend = Path(filepath).stem  # ファイル名から拡張子を除いた部分
        return filepath, legend

def plot_multiple_files(file_args, args):
    """複数ファイルをプロット"""
    
    # 図を作成
    plt.figure(figsize=(10, 6))
    
    # 色のリスト
    colors = ['#023E8A', '#812D3A', '#E3B935', '#005955', 'blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray']

    # マーカーのリスト
    markers = ['o', '^', 's', 'x', 'P', '*', '+']
    
    for i, file_arg in enumerate(file_args):
        # ファイルパスと凡例名を分離
        filepath, legend_name = parse_file_with_legend(file_arg)
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]
        
        # ファイルの存在確認
        if not Path(filepath).exists():
            print(f"警告: ファイルが見つかりません: {filepath}")
            continue
        
        # データを読み込み
        df = read_csv_data(filepath)
        if df.empty:
            print(f"警告: {filepath} にデータがありません")
            continue
        
        # 統計を計算
        stats = calculate_stats(df)
        
        # 散布図（全測定値）を薄く表示
        if not args.no_scatter:
          plt.scatter(df['X'], df['Y'], 
                    marker='.', s=2, alpha=0.3, color=lighten_color(color, 0.5))
        
        if args.no_errorbars:
          # 平均値を折れ線グラフで表示
          plt.plot(stats['X'], stats['mean'],
                      marker=marker, color=color, linewidth=1, 
                      markersize=4,
                      label=f'{legend_name} (mean)')
        else:
          # 平均値をエラーバー付きで表示
          plt.errorbar(stats['X'], stats['mean'], yerr=stats['error'],
                      fmt='o-', color=color, linewidth=1, 
                      markersize=4, capsize=2,
                      label=f'{legend_name} (mean ± 2σ)')
    
    # グラフの設定
    plt.xlabel(args.xlabel, fontsize=12, fontweight='bold')
    plt.ylabel(args.ylabel, fontsize=12, fontweight='bold')
    plt.title(args.title, fontsize=14, fontweight='bold')
    
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
    plt.tight_layout()
    plt.savefig(args.output, dpi=300, bbox_inches='tight')
    print(f"グラフを保存しました: {args.output}")

def lighten_color(color, factor=0.5):
    """
    任意の色を薄くする（matplotlib色名対応）
    
    Args:
        color (str): 色指定 ("red", "tab:blue", "#FF0000" など)
        factor (float): 0.0-1.0、1.0に近いほど薄くなる
    
    Returns:
        str: 薄くした色の "#RRGGBB" 形式
    """
    # matplotlib で RGB に変換
    r, g, b = mcolors.to_rgb(color)
    
    # 0-255 の整数に変換
    r = int(r * 255)
    g = int(g * 255)
    b = int(b * 255)
    
    # 白(255)に近づける
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    
    # 16進数で返す
    return f"#{r:02x}{g:02x}{b:02x}"

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description='シンプルな複数CSV実験データプロッター',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  %(prog)s data1.csv data2.csv
  %(prog)s exp1.csv=実験1 exp2.csv=実験2 --title "性能比較"
  %(prog)s data.csv --xlabel "サンプル数" --ylabel "時間[秒]" --yscale log
  %(prog)s results.csv --xmin 1 --xmax 1000 --ymin 0 --ymax 100
        """)
    
    # ファイル引数
    parser.add_argument('files', nargs='+',
                       help='CSVファイル（filename.csv=legend 形式で凡例指定可能）')
    
    # グラフ設定
    parser.add_argument('--title', default='Experimental Data Comparison',
                       help='グラフタイトル (デフォルト: Experimental Data Comparison)')
    parser.add_argument('--xlabel', default='X',
                       help='X軸ラベル (デフォルト: X)')
    parser.add_argument('--ylabel', default='Y',
                       help='Y軸ラベル (デフォルト: Y)')
    parser.add_argument('--no-errorbars', action='store_true',
                       help='エラーバー省略 (デフォルト: False)')
    parser.add_argument('--no-scatter', action='store_true',
                       help='散布図省略 (デフォルト: False)')
    
    # 軸範囲
    parser.add_argument('--xmin', type=float,
                       help='X軸最小値')
    parser.add_argument('--xmax', type=float,
                       help='X軸最大値')
    parser.add_argument('--ymin', type=float,
                       help='Y軸最小値')
    parser.add_argument('--ymax', type=float,
                       help='Y軸最大値')
    
    # スケール設定
    parser.add_argument('--xscale', choices=['linear', 'log'],
                       help='X軸スケール (linear または log)')
    parser.add_argument('--yscale', choices=['linear', 'log'],
                       help='Y軸スケール (linear または log)')
    
    # 出力設定
    parser.add_argument('--output', '-o', default='plot.png',
                       help='出力ファイル名 (デフォルト: plot.png)')
    
    args = parser.parse_args()
    
    print(f"処理するファイル: {len(args.files)}個")
    for file_arg in args.files:
        filepath, legend = parse_file_with_legend(file_arg)
        print(f"  {filepath} → {legend}")
    
    # プロット実行
    plot_multiple_files(args.files, args)

if __name__ == "__main__":
    main()