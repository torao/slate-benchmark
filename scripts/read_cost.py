# pip install numpy matplotlib
import numpy as np
import matplotlib.pyplot as plt
import math

def i_mod_2e(i: int, j: int) -> int:
  return i if j >= 64 else (i & ((1 << j) - 1))

def range_inclusive(i: int, j: int):
  # is_pbst(i, j) の代わりに常に False と仮定（仮実装）
  i_min = i - i_mod_2e(i - 1, j)
  i_max = i
  return range(i_min, i_max + 1)

def contains(i: int, j: int, k: int) -> bool:
  assert j <= 64
  return k in range_inclusive(i, j)

def floor_log2(x: int) -> int:
  assert x > 0
  return x.bit_length() - 1

def ceil_log2(x: int) -> int:
  assert x > 0
  return math.ceil(math.log2(x))

def pow2e(j: int) -> int:
  assert j < 64
  return 1 << j

def pbst_roots(n: int):
  remaining = n
  while remaining > 0:
    j = floor_log2(remaining)
    size = pow2e(j)
    i = n - remaining + size
    yield (i, j)
    remaining -= size

def entry_access_distance(k: int, n: int):
  for i, j in pbst_roots(n):
    if contains(i, j, k):
      start_val = range_inclusive(i, j)[0]
      cnt_ones = bin(k - start_val).count("1")
      return j - cnt_ones + (1 if i != n else 0)
  return None

def worst_best(n):
  h = ceil_log2(n)
  worst = [None] * (h + 1)
  best = [None] * (h + 1)
  for i in range(1, n + 1):
    d = entry_access_distance(i, n)
    if worst[d] is None:
      worst[d] = i
    best[d] = i
  
  pass

def main():
  #N = pow2e(8) + pow2e(6) + pow2e(5)
  N = pow2e(2)
  H = ceil_log2(N)
  ks = np.arange(1, N + 1, dtype=np.int64)
  ds = [entry_access_distance(k, N) for k in ks]

  FONT = "Neue Haas Grotesk Text Pro"
  plt.figure(figsize=(6, 4))
  plt.scatter(ks, ds, marker='o', facecolors='none', edgecolors='#A0C4FF', s=10)

  offset = 0
  ds_ul_x = [None] * (H + 1)
  ds_ul_y = [None] * (H + 1)
  ds_ll_x = [None] * (H + 1)
  ds_ll_y = [None] * (H + 1)
  for i, j in pbst_roots(N):
    n = pow2e(j)
    ks = np.arange(1, n + 1, dtype=np.int64)
    d_ul = np.floor(np.log2(n - ks + 1))
    d_ll = np.ceil(np.log2(n / ks))
    if i < N:
      d_ul = d_ul + 1
      d_ll = d_ll + 1
    ks = ks + offset
    plt.plot(ks, d_ul, color='#991C38', linewidth=1, zorder=3)
    plt.plot(ks, d_ll, color='#1C6ECD', linewidth=1, zorder=2)
    offset = i
    for k, d in zip(ks, d_ul):
      ds_ul_y[int(d)] = d
      ds_ul_x[int(d)] = k
    for k, d in zip(ks, d_ll):
      if ds_ll_y[int(d)] is None:
        ds_ll_y[int(d)] = d
        ds_ll_x[int(d)] = k

  plt.plot(ds_ul_x, ds_ul_y, drawstyle='steps-post', color='#991C38', marker='o', linewidth=3, alpha=0.25, zorder=3, label='$d\'_{{\\rm ul},i}$')
  plt.plot(ds_ll_x, ds_ll_y, drawstyle='steps-pre', color='#1C6ECD', marker='o', linewidth=3, alpha=0.25, zorder=2, label='$d\'_{{\\rm ll},i}$')

  plt.xlabel('Position $i$', fontname=FONT, fontsize=10)
  plt.ylabel('Number of I/O Reads $d$', fontname=FONT, fontsize=10)
  plt.title(f'Distribution of I/O Read ($T_{{{N}}}$)', fontname=FONT, fontsize=12)
  plt.grid(True, which='both', linestyle='--', alpha=0.7)
  plt.legend(fontsize=10)
  plt.tight_layout()
  plt.savefig(f'io_read_h{H:02}.png', dpi=300)

if __name__ == "__main__":
  main()
