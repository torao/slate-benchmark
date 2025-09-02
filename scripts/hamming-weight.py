#!/usr/bin/env python3
"""
ハミング重み（popcount）の分布を描画するプログラム

範囲 [0, 2^n) でハミング重みの上限、下限、期待値を折れ線グラフとして描画します。
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse
import sys


def popcount(x):
  """整数xのハミング重み（1のビット数）を返す"""
  return bin(x).count('1')


def hamming_weight_bounds(x, h):
  """
  ハミング重みの理論的な境界と期待値を計算
  
  Args:
      x: 数値
      h: ビット数
      
  Returns:
      tuple: (下限, 上限, 期待値)
  """
  # 上限: floor(log2(2^h-x))
  if x == 2**h - 1:
    upper_bound = 0  # 2^h-x = 1 の場合
  elif 2**h - x > 0:
    upper_bound = int(np.floor(np.log2(2**h - x)))
  else:
    upper_bound = 0
  
  # 下限: h - floor(log2(x+1))
  if x == 0:
    lower_bound = h - 1  # log2(1) = 0
  else:
    lower_bound = h - int(np.floor(np.log2(x + 1)))
  
  # 期待値: 下限 + (上限-下限)/2
  expected_value = lower_bound + (upper_bound - lower_bound) / 2.0
  
  return lower_bound, upper_bound, expected_value


def plot_hamming_weight_distribution(n):
  """
  ハミング重み分布をプロット
  
  Args:
      n: 最大ビット数（範囲は [0, 2^n)）
  """
  if n < 0:
    raise ValueError("n must be non-negative")
  
  if n > 20:
    print(f"Warning: n={n} is large. This may take a long time and use significant memory.")
    response = input("Continue? (y/N): ")
    if response.lower() != 'y':
      sys.exit(0)
  
  # 範囲 [0, 2^n) の数値
  x_values = np.arange(0, 2**n)
  
  # 各数値に対する境界と期待値、実際のハミング重みを計算
  lower_bounds = []
  upper_bounds = []
  expected_values = []
  actual_hamming_weights = []
  
  for x in x_values:
    lower, upper, expected = hamming_weight_bounds(x, n)
    actual = n - popcount(x)  # k - popcount(x)
    lower_bounds.append(lower)
    upper_bounds.append(upper)
    expected_values.append(expected)
    actual_hamming_weights.append(actual)
  
  # グラフの描画
  matplotlib.use('pgf')
  plt.rcParams.update({
    "text.usetex": True,
    "pgf.texsystem": "xelatex",
    "pgf.rcfonts": False,
    "pgf.preamble": r"\usepackage{fontspec}\setsansfont{Neue Haas Grotesk Display Pro}"
  })
  plt.figure(figsize=(8, 5))
  plt.scatter(x_values, actual_hamming_weights, marker='o', facecolors='none', edgecolors='#A0C4FF', s=20, label='Actual \\# of Zeros $c(x)$')
  #plt.plot(x_values, actual_hamming_weights, 'ko', label='Actual hamming weight', markersize=1, alpha=0.7)
  plt.plot(x_values, lower_bounds, '-', label='Lower bound $c_{\\rm ll}(x)$', linewidth=1, color='#1C6ECD')
  plt.plot(x_values, upper_bounds, '-', label='Upper bound $c_{\\rm ul}(x)$', linewidth=1, color='#991C38')
  plt.plot(x_values, expected_values, '--', label='Expected value $\\bar{c}(x)$', linewidth=1, color="#005955")
  
  plt.xlabel('Leaf Node $x$')
  plt.ylabel(f'Number of Zeros in $x$')
  plt.title(f'Distribution of the Number of Zeros in $2^{{{n}}}$ representation')
  plt.legend()
  plt.grid(True, alpha=0.3)
  #plt.xlim(left=0)
  #plt.ylim(bottom=0)
  
  # 整数の目盛りを設定
  plt.xticks(np.arange(0, 2**n, max(1, 2**n // 10)))
  plt.yticks(np.arange(0, n + 1, max(1, n // 10)))
  
  plt.tight_layout()
  
  # ファイルに保存
  filename = f"hamming-weight-{n:02}.png"
  plt.savefig(filename, dpi=300, bbox_inches='tight')
  print(f"グラフを {filename} に保存しました。")


def main():
  """メイン関数"""
  parser = argparse.ArgumentParser(
    description='ハミング重み（popcount）の分布を描画します。'
  )
  parser.add_argument(
    'n', 
    type=int, 
    help='最大ビット数（範囲は [0, 2^n)）'
  )
  
  args = parser.parse_args()
  
  try:
    plot_hamming_weight_distribution(args.n)
  except ValueError as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
  except KeyboardInterrupt:
    print("\n操作がキャンセルされました。")
    sys.exit(0)


if __name__ == '__main__':
  main()