#!/usr/bin/env python3
"""
CSV データを読み込んで gnuplot で xy プロットと ±2σ エラーバーを描画するプログラム

CSV 形式:
X,Y,...
"1", 100, 102, 101, 98, 100, 98
"2", 150, 151, 150, 149, 150
...
"""

import csv
import subprocess
import tempfile
import os
import numpy as np
from pathlib import Path
import argparse


class GnuplotDataProcessor:
  """CSV データを処理して gnuplot 用データを生成するクラス"""
  
  def __init__(self, csv_file):
    self.x_max = 0
    self.csv_file = csv_file
    self.raw_data = []
    self.summary_data = []
    
  def read_csv_data(self):
    """CSV ファイルを読み込んで生データを抽出"""
    self.raw_data = []
    
    with open(self.csv_file, 'r', encoding='utf-8') as file:
      reader = csv.reader(file)
      header = next(reader)  # ヘッダーをスキップ
      
      for row in reader:
        if not row or not row[0].strip():  # 空行をスキップ
          continue
          
        try:
          # X値を取得（クォートを除去）
          x_value = float(row[0].strip('"\''))
          self.x_max = x_value if x_value > self.x_max else self.x_max
          
          # Y値を取得（空文字列や無効な値をスキップ）
          y_values = []
          for y_str in row[1:]:
            y_str = y_str.strip()
            if y_str:  # 空文字列でない場合
              try:
                y_values.append(float(y_str))
              except ValueError:
                continue  # 無効な数値はスキップ
          
          if y_values:  # Y値が1つ以上ある場合のみ追加
            for y in y_values:
              self.raw_data.append((x_value, y))
              
        except (ValueError, IndexError) as e:
          print(f"Warning: Skipping row: {row} (Reason: {e})")
          continue
    
    print(f"Loaded data points: {len(self.raw_data)}")
    
  def calculate_statistics(self):
    """各 X 値に対する統計値を計算"""
    # X値でグループ化
    x_groups = {}
    for x, y in self.raw_data:
      if x not in x_groups:
        x_groups[x] = []
      x_groups[x].append(y)
    
    # 統計値を計算
    self.summary_data = []
    for x in sorted(x_groups.keys()):
      y_values = np.array(x_groups[x])
      
      mean = np.mean(y_values)
      std = np.std(y_values, ddof=1) if len(y_values) > 1 else 0.0
      error_2sigma = 2 * std  # ±2σ
      count = len(y_values)
      
      self.summary_data.append({
        'x': x,
        'mean': mean,
        'std': std,
        'error_2sigma': error_2sigma,
        'count': count,
        'y_values': y_values
      })
    
    print(f"Number of unique X values: {len(self.summary_data)}")
    
  def write_gnuplot_files(self):
    """gnuplot 用のデータファイルを作成"""
    # 生データファイル
    raw_data_file = "raw_data.txt"
    with open(raw_data_file, 'w') as f:
      f.write("# X Y\n")
      for x, y in self.raw_data:
        f.write(f"{x} {y}\n")
    
    # 統計データファイル
    summary_data_file = "summary_data.txt"
    with open(summary_data_file, 'w') as f:
      f.write("# X Mean Error_2sigma Count\n")
      for data in self.summary_data:
        f.write(f"{data['x']} {data['mean']} {data['error_2sigma']} {data['count']}\n")
    
    return raw_data_file, summary_data_file
    
  def generate_gnuplot_script(self, raw_data_file, summary_data_file, output_file=None):
    """gnuplot スクリプトを生成"""
    script = f'''
# Gnuplot script for xy plot with error bars
reset

# 出力設定
{f'set terminal pngcairo enhanced font "Arial,12" size 1000,700' if output_file else 'set terminal qt enhanced font "Arial,12" size 1000,700'}
{f'set output "{output_file}"' if output_file else ''}

# グラフの基本設定
set title "Plot" font ",14"
set xlabel "distance from the latest" font ",12"
set ylabel "time [msec]" font ",12"
set grid
set key top left
set key box opaque width 2 height 1
set xrange [0:256]
set yrange [0:0.04]

# エラーバーのスタイル設定
set style line 1 lc rgb '#87CEEB' pt 7 ps 0.4   # 個別データ点（ライトブルー）
set style line 2 lc rgb '#812D3A' pt 9 ps 1.0 lw 2       # 平均値（赤）
set style line 3 lc rgb '#812D3A' lw 2                   # エラーバー（赤）

f(x) = a * log(x) + b
fit f(x) "{raw_data_file}" using 1:2 via a,b

# プロット実行
plot "{raw_data_file}" using 1:2 with points ls 1 title "Measurements", \\
     "{summary_data_file}" using 1:2:3 with points ls 2 title "Mean", \\
     f(x) with lines lc rgb '#c6001c' title sprintf("Fit: y = %.4f x log(x) + %.4f",a,b)

{'' if output_file else 'pause -1 "Press Enter to close..."'}
'''
    return script.strip()

  def create_plot(self, output_file=None, show_stats=True):
    """プロットを作成"""
    # 出力ファイル名が指定されていない場合、入力ファイル名から生成
    if output_file is None:
      input_path = Path(self.csv_file)
      if input_path.suffix.lower() == '.csv':
        output_file = str(input_path.with_suffix('.png'))
      else:
        output_file = str(input_path.with_suffix('.png'))
      print(f"Output filename: {output_file}")
    
    # データファイルを作成
    raw_data_file, summary_data_file = self.write_gnuplot_files()
    
    # 統計情報を表示
    if show_stats:
      self.print_statistics()
    
    # gnuplot スクリプトを生成
    script = self.generate_gnuplot_script(raw_data_file, summary_data_file, output_file)
    
    # gnuplot を実行
    try:
      process = subprocess.Popen(['gnuplot'], stdin=subprocess.PIPE, text=True)
      process.communicate(input=script)
      
      print(f"Plot saved to {output_file}")
      
      # デバッグ用：生成されたファイルも表示
      print(f"Generated files:")
      print(f"  - Raw data: {raw_data_file}")
      print(f"  - Summary data: {summary_data_file}")
      print(f"  - Gnuplot script: plot_script.gp")
      
      # スクリプトファイルとして保存（デバッグ用）
      with open("plot_script.gp", 'w') as f:
        f.write(script)
        
    except FileNotFoundError:
      print("エラー: gnuplot が見つかりません。gnuplot をインストールしてください。")
      print("Ubuntu/Debian: sudo apt install gnuplot")
      print("作成されたファイル:")
      print(f"  - 生データ: {raw_data_file}")
      print(f"  - 統計データ: {summary_data_file}")
      print(f"  - gnuplot スクリプト: plot_script.gp")
      
      # スクリプトファイルとして保存
      with open("plot_script.gp", 'w') as f:
        f.write(script)
    
    # 一時ファイルを削除（オプション）
    # os.remove(raw_data_file)
    # os.remove(summary_data_file)
    
  def print_statistics(self):
    """統計情報を表示"""
    print(f"\n=== Statistics ===")
    print("X     Mean      StdDev    +/-2sigma  Count")
    print("-" * 50)
    
    for data in self.summary_data:
      print(f"{data['x']:4.1f}  {data['mean']:8.2f}  {data['std']:8.2f}  {data['error_2sigma']:8.2f}  {data['count']:6d}")
    
    # 全体統計
    all_x = [x for x, _ in self.raw_data]
    all_y = [y for _, y in self.raw_data]
    print(f"\nOverall Statistics:")
    print(f"  Total data points: {len(all_y)}")
    print(f"  X range: {np.min(all_x):.2f} ~ {np.max(all_x):.2f}")
    print(f"  Y range: {np.min(all_y):.2f} ~ {np.max(all_y):.2f}")
    print(f"  Overall mean: {np.mean(all_y):.2f}")
    print(f"  Overall std dev: {np.std(all_y, ddof=1):.2f}")
    
    # 異常値チェック
    print(f"\n=== Anomaly Check ===")
    print(f"  NaN values: {sum(1 for y in all_y if np.isnan(y))}")
    print(f"  Infinite values: {sum(1 for y in all_y if np.isinf(y))}")
    print(f"  Very large values (>1e6): {sum(1 for y in all_y if abs(y) > 1e6)}")
    print(f"  Very small values (<1e-6): {sum(1 for y in all_y if abs(y) < 1e-6 and y != 0)}")


def create_sample_csv(filename="sample_data.csv"):
  """サンプル CSV ファイルを作成"""
  import random
  
  with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['X', 'Y', '...'])  # ヘッダー
    
    # サンプルデータを生成
    for x in range(1, 6):
      # 各 X に対して 5-8 個の Y 値を生成（正規分布）
      n_points = random.randint(5, 8)
      base_y = 100 + x * 20  # 基準値
      y_values = [f'"{x}"'] + [
        f"{base_y + random.gauss(0, 5):.1f}" 
        for _ in range(n_points)
      ]
      writer.writerow(y_values)
  
  print(f"Sample CSV file '{filename}' created")


def main():
  parser = argparse.ArgumentParser(description='Create errorbar plots from CSV data using gnuplot')
  parser.add_argument('csv_file', nargs='?', help='Input CSV file')
  parser.add_argument('-o', '--output', help='Output image filename (default: input_filename.png)')
  parser.add_argument('--sample', action='store_true', help='Create sample CSV file')
  parser.add_argument('--no-stats', action='store_true', help='Skip statistics display')
  
  args = parser.parse_args()
  
  # サンプルファイル作成
  if args.sample:
    create_sample_csv()
    return
  
  # CSV ファイルの存在確認
  if not args.csv_file:
    print("Error: Please specify a CSV file")
    print("Usage: python script.py data.csv")
    print("Create sample: python script.py --sample")
    return
  
  if not Path(args.csv_file).exists():
    print(f"Error: File '{args.csv_file}' not found")
    return
  
  # データ処理とプロット作成
  try:
    processor = GnuplotDataProcessor(args.csv_file)
    processor.read_csv_data()
    
    if not processor.raw_data:
      print("Error: No valid data found")
      return
    
    processor.calculate_statistics()
    processor.create_plot(output_file=args.output, show_stats=not args.no_stats)
    
  except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()


if __name__ == "__main__":
  main()