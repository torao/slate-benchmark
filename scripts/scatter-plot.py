#!/usr/bin/env python3
"""
CSV エラーバープロット生成ツール（スケール選択機能付き）
==========================================

【目的】
複数測定値を含む特殊CSV形式のデータから、統計解析とエラーバー付きグラフを生成する。
実験データの可視化、性能測定結果の分析、品質管理データの表示などに使用。
X軸・Y軸それぞれで線形スケールと対数スケールを選択可能。

【入力データ形式】
CSVファイル（ヘッダー行 + データ行）:
```
N,Y
"1",104,103,99,103,101
"2",153,152,155,149
"3",198,202,201,208,200,202,203
```
- 1行目: ヘッダー（無視される）
- 1列目: X値（独立変数、例：距離、時間、条件番号）
- 2列目: Y値（複数測定値がカンマ区切り、例：応答時間、測定値）

【出力】
1. PNG画像ファイル（入力ファイル名.png）
   - 個別データ点の散布図（薄いグレー）
   - 各X値での平均値とエラーバー（±2σ）
   - 統計情報ボックス
   - 凡例
2. 統計CSVファイル（入力ファイル名_statistics.csv）
   - X値ごとの統計量（平均、標準偏差、データ数など）

【処理フロー】
1. CSV読み込み → 2列目の複数値を個別行に展開
2. X値でグループ化 → 統計計算（平均、±2σ）
3. グラフ生成（散布図 + エラーバー付き平均値プロット）
4. 高解像度PNG保存

【主要オプション】
--xlim, --ylim: 軸範囲指定
--xscale, --yscale: スケール選択（linear/log）
--line-width: 平均値ライン太さ（0=非表示）
--errorbar-width: エラーバー太さ（0=非表示、--no-errorbarsの代替）
--marker-size: 平均値マーカーサイズ
--mean-color, --data-color: 色指定（#RRGGBB形式）
--stats-position, --legend-position: 配置指定（4隅）
--title, --xlabel, --ylabel: ラベル指定

【使用例】
python script.py data.csv --xscale log --yscale linear --errorbar-width 0
python script.py data.csv --xscale linear --yscale log --mean-color '#0000FF'

【技術詳細】
- pandas: CSV解析、データ操作
- matplotlib: グラフ生成
- numpy: 統計計算
- 手動CSV解析: 特殊形式対応（pandas標準機能では解析困難）
- エラーバー: ±2σ（95%信頼区間近似）
- ジッター: 同一X値での重複回避
- 対数スケール: 正の値のみ有効（自動フィルタリング）
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import sys
from typing import Tuple, Dict, List
import re

class SpecialCSVDataAnalyzer:
  """
  特殊CSV形式のデータ解析とエラーバープロット生成クラス
  1列目=X値、2列目以降=Y値（複数測定値がカンマ区切り）
  """
  
  def __init__(self):
    """アナライザーの初期化"""
    plt.rcParams['font.family'] = 'DejaVu Sans'
    self.raw_data = None
    self.processed_data = None
    self.statistics = None
    
  def load_and_parse_csv(self, filepath: str) -> pd.DataFrame:
    """CSV読み込みと解析"""
    try:
      print(f"ファイル読み込み: {filepath}")
      
      # CSVファイルを行ごとに読み込み
      with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
      
      # ヘッダー行をスキップ
      data_lines = lines[1:] if len(lines) > 1 else []
      
      print(f"総行数: {len(lines)} (ヘッダー除く: {len(data_lines)})")
      
      # 最初の数行を確認
      print("最初の3行の生データ:")
      for i, line in enumerate(lines[:4]):  # ヘッダー含む4行
        print(f"  行{i}: {line.strip()[:80]}")
      
      # 手動でデータを解析
      raw_data = []
      for line_num, line in enumerate(data_lines):
        try:
          # 行をパース
          line = line.strip()
          if not line:
            continue
            
          # カンマ区切りで分割
          parts = [part.strip().replace('"', '') for part in line.split(',')]
          
          if len(parts) >= 2:
            raw_data.append(parts)
          
        except Exception as e:
          print(f"行{line_num+2}の解析エラー: {e}")
          continue
      
      if not raw_data:
        raise ValueError("解析可能なデータが見つかりませんでした")
      
      # DataFrameを作成
      max_cols = max(len(row) for row in raw_data)
      
      # 列名を生成
      columns = ['X'] + [f'Y_{i}' for i in range(1, max_cols)]
      
      # データを統一長に調整
      normalized_data = []
      for row in raw_data:
        normalized_row = row + [''] * (max_cols - len(row))
        normalized_data.append(normalized_row)
      
      self.raw_data = pd.DataFrame(normalized_data, columns=columns)
      
      print(f"解析完了: {self.raw_data.shape[0]}行 × {self.raw_data.shape[1]}列")
      #print(f"列名: {list(self.raw_data.columns)}")
      
      # 最初の数行を確認
      print("解析後のデータ:")
      print(self.raw_data.head(3))
      
      # データの解析と変換
      self.processed_data = self._parse_measurement_data()
      return self.processed_data
      
    except Exception as e:
      raise ValueError(f"データ読み込みエラー: {e}")
  
  def _parse_measurement_data(self) -> pd.DataFrame:
    """
    解析済みデータから測定値を展開
    1列目をX値、2列目以降をY値として処理
    """
    expanded_data = []
    
    print(f"データ展開: X列=1列目, Y値=2列目以降")
    
    for row_num, (_, row) in enumerate(self.raw_data.iterrows()):
      try:
        # 1列目をX値として使用（文字列から数値に変換）
        x_str = str(row.iloc[0]).strip()
        if not x_str or x_str == '':
          continue
          
        x_value = float(x_str)
        
        # 2列目以降をY値として処理
        y_values_all = []
        for col_idx in range(1, len(row)):
          cell_value = str(row.iloc[col_idx]).strip()
          
          if cell_value and cell_value != '' and cell_value != 'nan':
            # セル内の値を解析（カンマ区切りまたは単一値）
            y_values = self._extract_numbers_from_string(cell_value)
            y_values_all.extend(y_values)
        
        # Y値が見つからない場合はスキップ
        if not y_values_all:
          continue
        
        # 各測定値を個別行として追加
        for measurement_idx, y_value in enumerate(y_values_all):
          expanded_data.append({
            'X': x_value,
            'Y': y_value,
            'measurement_index': measurement_idx + 1
          })
        
        # 最初の数行をデバッグ出力
        if row_num < 3:
          print(f"  行{row_num+1}: X={x_value}, Y値数={len(y_values_all)}")
            
      except Exception as e:
        print(f"行{row_num+1}の処理中にエラー: {e}")
        continue
    
    if not expanded_data:
      raise ValueError("処理可能なデータが見つかりませんでした")
    
    result_df = pd.DataFrame(expanded_data)
    
    print(f"展開完了: {len(result_df)}個の測定点")
    print(f"X値範囲: {result_df['X'].min()} - {result_df['X'].max()}")
    print(f"Y値範囲: {result_df['Y'].min():.4f} - {result_df['Y'].max():.4f}")
    
    return result_df
  
  def _extract_numbers_from_string(self, text: str) -> List[float]:
    """文字列から数値を抽出"""
    # クォートを除去
    text = text.replace('"', '').replace("'", '')
    
    # まず単一の数値かチェック
    try:
      return [float(text.strip())]
    except ValueError:
      pass
    
    # カンマ区切りで分割して数値に変換
    parts = [part.strip() for part in text.split(',')]
    numbers = []
    
    for part in parts:
      try:
        # 数値パターンのマッチング
        number_match = re.search(r'-?\d+\.?\d*', part)
        if number_match:
          numbers.append(float(number_match.group()))
      except (ValueError, AttributeError):
        continue
    
    return numbers
  
  def _filter_data_for_scale(self, data: pd.DataFrame, xscale: str, yscale: str) -> pd.DataFrame:
    """スケールに応じてデータをフィルタリング"""
    filtered_data = data.copy()
    original_count = len(filtered_data)
    
    # 対数スケールの場合、正の値のみを保持
    if xscale == 'log':
      filtered_data = filtered_data[filtered_data['X'] > 0]
      x_filtered_count = original_count - len(filtered_data)
      if x_filtered_count > 0:
        print(f"警告: X軸対数スケールのため、X≤0の{x_filtered_count}個のデータ点を除外しました")
    
    if yscale == 'log':
      filtered_data = filtered_data[filtered_data['Y'] > 0]
      y_filtered_count = len(data) - len(filtered_data)
      if y_filtered_count > 0:
        print(f"警告: Y軸対数スケールのため、Y≤0の{y_filtered_count}個のデータ点を除外しました")
    
    if len(filtered_data) == 0:
      raise ValueError("スケール設定により全てのデータが除外されました")
    
    return filtered_data
  
  def calculate_statistics(self, xscale: str = 'linear', yscale: str = 'linear') -> Dict:
    """各X値での統計量を計算（スケールを考慮）"""
    if self.processed_data is None:
      raise ValueError("データが解析されていません")
    
    # スケールに応じてデータをフィルタリング
    filtered_data = self._filter_data_for_scale(self.processed_data, xscale, yscale)
    
    # X値でグループ化して統計量を計算
    stats = filtered_data.groupby('X')['Y'].agg([
      'count', 'mean', 'std', 'min', 'max', 'median'
    ]).reset_index()
    
    # NaNの標準偏差を0に置換
    stats['std'] = stats['std'].fillna(0)
    
    # ±2σ範囲の計算
    stats['error_2sigma'] = 2 * stats['std']
    
    # 変動係数の計算
    stats['cv'] = (stats['std'] / stats['mean']) * 100
    
    # 対数スケールの場合の特別な処理
    if yscale == 'log':
      # 対数スケールでのエラーバーは乗法的エラーとして処理
      # log(mean ± σ) ≈ log(mean) ± σ/mean （小さなσの場合）
      relative_error = stats['std'] / stats['mean']
      stats['log_error_upper'] = stats['mean'] * (1 + relative_error * 2)
      stats['log_error_lower'] = np.maximum(stats['mean'] * (1 - relative_error * 2), 
                                           stats['mean'] * 0.01)  # 最小値制限
    
    self.statistics = stats
    self.filtered_data = filtered_data
    
    print(f"統計計算完了: {len(stats)}個のX値")
    print(f"使用データ点数: {len(filtered_data)}")
    
    return {'statistics': stats, 'processed_data': filtered_data}
  
  def create_plot(self, output_path: str, figsize: Tuple[float, float] = (12, 8), 
                 dpi: int = 300, ylim: Tuple[float, float] = None, 
                 xlim: Tuple[float, float] = None, show_errorbars: bool = True,
                 title: str = "Data Points with Statistical Error Bars",
                 xlabel: str = "Distance from Latest Entry",
                 ylabel: str = "Data Acquisition Time [msec]",
                 show_annotations: bool = False, line_width: float = 2.0,
                 stats_position: str = "bottom-left", errorbar_width: float = 2.0,
                 legend_position: str = "bottom-left", marker_size: float = 8.0,
                 mean_color: str = "#812D3A", data_color: str = "#87CFEB",
                 xscale: str = 'linear', yscale: str = 'linear') -> None:
    """単一グラフに全データとエラーバーを描画（スケール設定対応）"""
    if self.statistics is None:
      raise ValueError("統計計算が実行されていません")
    
    if not hasattr(self, 'filtered_data'):
      raise ValueError("フィルタリングされたデータが存在しません")
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # スケール設定
    ax.set_xscale(xscale)
    ax.set_yscale(yscale)
    
    print(f"スケール設定: X軸={xscale}, Y軸={yscale}")
    
    # 個別データポイントをプロット（指定色）
    unique_x_values = sorted(self.filtered_data['X'].unique())
    
    for x_val in unique_x_values:
      x_data = self.filtered_data[self.filtered_data['X'] == x_val]
      
      # ジッター効果の調整（対数スケールの場合）
      if xscale == 'log':
        # 対数スケールでは相対的なジッターを使用
        jitter_factor = 0.02  # 2%の相対ジッター
        jitter = np.random.normal(1.0, jitter_factor, len(x_data))
        x_positions = x_data['X'] * jitter
      else:
        # 線形スケールでは絶対的なジッターを使用
        jitter = np.random.normal(0, 0.01, len(x_data))
        x_positions = x_data['X'] + jitter
      
      ax.scatter(x_positions, x_data['Y'], alpha=0.4, s=20, color=data_color, zorder=1)
    
    # エラーバー付き平均値プロット
    x_vals = self.statistics['X']
    y_means = self.statistics['mean']
    
    # エラーバーの制御（太さ0の場合は非表示）
    show_errorbars_actual = show_errorbars and errorbar_width > 0
    
    if show_errorbars_actual:
      if yscale == 'log' and 'log_error_upper' in self.statistics.columns:
        # 対数スケール用の非対称エラーバー
        y_errors_lower = y_means - self.statistics['log_error_lower']
        y_errors_upper = self.statistics['log_error_upper'] - y_means
        y_errors = [y_errors_lower, y_errors_upper]
      else:
        # 線形スケール用の対称エラーバー
        y_errors = self.statistics['error_2sigma']
      
      # エラーバー付きライン
      ax.errorbar(x_vals, y_means, yerr=y_errors,
                 fmt='o-' if line_width > 0 else 'o',
                 color=mean_color, capsize=5, capthick=2, elinewidth=errorbar_width,
                 markersize=marker_size, linewidth=line_width if line_width > 0 else 0, 
                 label='Mean +/- 2sigma', alpha=0.9, zorder=3)
      
      # 2σ範囲のシェード（線形スケールのみ）
      if yscale == 'linear':
        ax.fill_between(x_vals, 
                       y_means - self.statistics['error_2sigma'], 
                       y_means + self.statistics['error_2sigma'],
                       alpha=0.2, color=mean_color, label='2sigma Range', zorder=2)
    else:
      # エラーバーなしの場合
      if line_width > 0:
        # ライン付きプロット
        ax.plot(x_vals, y_means, 'o-', color=mean_color, markersize=marker_size, 
                linewidth=line_width, label='Mean', alpha=0.9, zorder=3)
      else:
        # マーカーのみ
        ax.plot(x_vals, y_means, 'o', color=mean_color, markersize=marker_size, 
                label='Mean', alpha=0.9, zorder=3)
    
    # データ点数の注釈（オプション制御）
    if show_annotations:
      for _, row in self.statistics.iterrows():
        n_count = int(row['count'])
        cv_value = row['cv']
        
        annotation_text = f'n={n_count}'
        if cv_value < 999:
          annotation_text += f'\nCV={cv_value:.1f}%'
        
        # 対数スケールでの注釈位置調整
        if yscale == 'log':
          y_offset = row['mean'] * 1.1  # 相対的な位置
        else:
          y_offset = row['mean'] + row['std']  # 絶対的な位置
        
        ax.annotate(annotation_text, 
                   (row['X'], y_offset),
                   xytext=(5, 10), textcoords='offset points',
                   fontsize=8, alpha=0.8,
                   bbox=dict(boxstyle='round,pad=0.2', 
                            facecolor='white', alpha=0.8, edgecolor='gray'),
                   ha='left')
    
    # グラフの設定
    ax.set_xlabel(xlabel, fontsize=12, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # 凡例の位置設定
    legend_position_map = {
      'bottom-left': 'lower left',
      'bottom-right': 'lower right',
      'top-left': 'upper left',
      'top-right': 'upper right'
    }
    legend_loc = legend_position_map.get(legend_position, 'lower left')
    ax.legend(loc=legend_loc, fontsize=11)
    
    ax.grid(True, alpha=0.3)
    
    # 軸範囲の設定（対数スケールの場合の検証）
    if xlim is not None:
      if xscale == 'log' and (xlim[0] <= 0 or xlim[1] <= 0):
        print("警告: X軸対数スケールでは正の値のみ有効です。xlim設定を無視します。")
      else:
        ax.set_xlim(xlim)
        print(f"X軸範囲を設定: {xlim[0]} - {xlim[1]}")
    
    if ylim is not None:
      if yscale == 'log' and (ylim[0] <= 0 or ylim[1] <= 0):
        print("警告: Y軸対数スケールでは正の値のみ有効です。ylim設定を無視します。")
      else:
        ax.set_ylim(ylim)
        print(f"Y軸範囲を設定: {ylim[0]} - {ylim[1]}")
    
    # 統計情報の追加（グラフ枠内に配置）
    stats_text = self._generate_statistics_text(xscale, yscale)
    
    # 統計情報の位置を決定（グラフ枠内に配置）
    position_map = {
      'bottom-left': (0.02, 0.02),
      'bottom-right': (0.98, 0.02),
      'top-left': (0.02, 0.98),
      'top-right': (0.98, 0.98)
    }
    
    if stats_position in position_map:
      x_pos, y_pos = position_map[stats_position]
      h_align = 'left' if 'left' in stats_position else 'right'
      v_align = 'bottom' if 'bottom' in stats_position else 'top'
      
      # グラフ枠内に配置（transform=ax.transAxes使用）
      ax.text(x_pos, y_pos, stats_text, fontsize=9, 
              bbox=dict(boxstyle='round,pad=0.4', facecolor='lightblue', alpha=0.8),
              verticalalignment=v_align, horizontalalignment=h_align,
              transform=ax.transAxes)
    
    plt.tight_layout()
    
    # 保存
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight', 
               facecolor='white', edgecolor='none')
    
    print(f"プロット保存: {output_path} (解像度: {dpi} DPI)")
    print(f"平均値ライン太さ: {line_width} ポイント")
    print(f"エラーバー太さ: {errorbar_width} ポイント")
    print(f"マーカーサイズ: {marker_size} ポイント")
    print(f"平均値色: {mean_color}")
    print(f"データ点色: {data_color}")
    print(f"統計情報位置: {stats_position}")
    print(f"凡例位置: {legend_position}")
    
    if show_errorbars_actual:
      print("エラーバー: 表示")
    else:
      print("エラーバー: 非表示")
    
    if show_annotations:
      print("データ注釈: 表示")
    else:
      print("データ注釈: 非表示")
    
    plt.close()
  
  def _generate_statistics_text(self, xscale: str = 'linear', yscale: str = 'linear') -> str:
    """統計情報テキスト生成（スケール情報付き）"""
    if self.statistics is None or not hasattr(self, 'filtered_data'):
      return ""
    
    total_points = len(self.filtered_data)
    unique_x = len(self.statistics)
    avg_points_per_x = total_points / unique_x if unique_x > 0 else 0
    
    overall_mean = self.filtered_data['Y'].mean()
    overall_std = self.filtered_data['Y'].std()
    
    x_min, x_max = self.statistics['X'].min(), self.statistics['X'].max()
    mean_cv = self.statistics['cv'].mean()
    
    return f"""Statistics Summary:
Total points: {total_points}
X range: {x_min} - {x_max}
Avg per X: {avg_points_per_x:.1f}
Overall mean: {overall_mean:.4f}
Overall std: {overall_std:.4f}
Mean CV: {mean_cv:.1f}%
Scale: X={xscale}, Y={yscale}"""
  
  def save_statistics_csv(self, output_path: str) -> None:
    """統計結果をCSVで保存"""
    if self.statistics is None:
      raise ValueError("統計計算が実行されていません")
    
    stats_output = self.statistics.copy()
    
    # 基本的な列名マッピング
    column_mapping = {
      'X': 'X_value',
      'count': 'count', 
      'mean': 'mean',
      'std': 'std_dev',
      'min': 'min',
      'max': 'max', 
      'median': 'median',
      'error_2sigma': 'error_2sigma',
      'cv': 'cv_percent'
    }
    
    # 対数スケール用の列があれば追加
    if 'log_error_upper' in self.statistics.columns:
      column_mapping['log_error_upper'] = 'log_error_upper'
      column_mapping['log_error_lower'] = 'log_error_lower'
    
    # 実際に存在する列のみでマッピングを実行
    new_columns = []
    for old_col in stats_output.columns:
      if old_col in column_mapping:
        new_columns.append(column_mapping[old_col])
      else:
        new_columns.append(old_col)  # マッピングされていない列はそのまま
    
    stats_output.columns = new_columns
    
    stats_output.to_csv(output_path, index=False)
    print(f"統計結果保存: {output_path}")
    print(f"保存された列: {list(stats_output.columns)}")

def validate_color(color_str: str) -> bool:
  """#RRGGBB形式の色コードを検証"""
  return bool(re.match(r'^#[0-9A-Fa-f]{6}$', color_str))

def validate_scale(scale_str: str) -> bool:
  """スケール設定を検証"""
  return scale_str.lower() in ['linear', 'log']

def process_csv_file(input_filepath: str, 
                    create_stats_csv: bool = True,
                    figsize: Tuple[float, float] = (12, 8),
                    dpi: int = 300,
                    ylim: Tuple[float, float] = None,
                    xlim: Tuple[float, float] = None,
                    show_errorbars: bool = True,
                    title: str = "Data Points with Statistical Error Bars",
                    xlabel: str = "Distance from Latest Entry",
                    ylabel: str = "Data Acquisition Time [msec]",
                    show_annotations: bool = False,
                    line_width: float = 2.0,
                    stats_position: str = "bottom-left",
                    errorbar_width: float = 2.0,
                    legend_position: str = "bottom-left",
                    marker_size: float = 8.0,
                    mean_color: str = "#812D3A",
                    data_color: str = "#87CFEB",
                    xscale: str = 'linear',
                    yscale: str = 'linear') -> None:
  """CSVファイルの処理メイン関数（スケール設定対応）"""
  
  print(f"処理開始: {input_filepath}")
  print("=" * 50)
  
  input_path = Path(input_filepath)
  if not input_path.exists():
    raise FileNotFoundError(f"ファイルが見つかりません: {input_filepath}")
  
  # 出力ファイル名生成
  base_name = input_path.stem
  output_dir = input_path.parent
  
  output_png_path = output_dir / f"{base_name}.png"
  output_stats_csv_path = output_dir / f"{base_name}_statistics.csv"
  
  try:
    # データ処理
    analyzer = SpecialCSVDataAnalyzer()
    analyzer.load_and_parse_csv(str(input_path))
    analyzer.calculate_statistics(xscale=xscale, yscale=yscale)
    analyzer.create_plot(str(output_png_path), figsize=figsize, dpi=dpi, 
                        ylim=ylim, xlim=xlim, show_errorbars=show_errorbars,
                        title=title, xlabel=xlabel, ylabel=ylabel,
                        show_annotations=show_annotations, line_width=line_width,
                        stats_position=stats_position, errorbar_width=errorbar_width,
                        legend_position=legend_position, marker_size=marker_size,
                        mean_color=mean_color, data_color=data_color,
                        xscale=xscale, yscale=yscale)
    
    # 統計CSV保存
    if create_stats_csv:
      analyzer.save_statistics_csv(str(output_stats_csv_path))
    
    print("=" * 50)
    print("処理完了!")
    print(f"出力画像: {output_png_path}")
    if create_stats_csv:
      print(f"統計CSV: {output_stats_csv_path}")
    
  except Exception as e:
    print(f"エラー: {e}")
    raise

def main():
  """メイン実行関数"""
  if len(sys.argv) < 2:
    print("使用方法: python script.py <input_csv_file> [options]")
    print("\n例:")
    print("  python script.py data.csv")
    print("  python script.py data.csv --title 'My Results' --mean-color '#0000FF'")
    print("  python script.py data.csv --marker-size 12 --data-color '#808080'")
    print("  python script.py data.csv --legend-position top-left")
    print("  python script.py data.csv --xscale log --yscale linear")
    print("  python script.py data.csv --xscale linear --yscale log")
    print("  python script.py data.csv --xscale log --yscale log")
    print("\nオプション:")
    print("  --no-stats-csv           統計CSVファイルを作成しない")
    print("  --dpi <value>            解像度設定 (デフォルト: 300)")
    print("  --size <w> <h>           図サイズ設定 (デフォルト: 12 8)")
    print("  --ymin <value>           Y軸最小値を指定")
    print("  --ymax <value>           Y軸最大値を指定")
    print("  --ylim <min> <max>       Y軸範囲を指定")
    print("  --xmin <value>           X軸最小値を指定")
    print("  --xmax <value>           X軸最大値を指定")
    print("  --xlim <min> <max>       X軸範囲を指定")
    print("  --xscale <scale>         X軸スケール (linear/log, デフォルト: linear)")
    print("  --yscale <scale>         Y軸スケール (linear/log, デフォルト: linear)")
    print("  --annotations            データ注釈 (n=数値, CV=値) を表示")
    print("  --title '<text>'         グラフタイトルを指定")
    print("  --xlabel '<text>'        X軸ラベルを指定")
    print("  --ylabel '<text>'        Y軸ラベルを指定")
    print("  --line-width <value>     平均値ライン太さ (0=非表示, デフォルト: 2)")
    print("  --errorbar-width <value> エラーバー太さ (0=非表示, デフォルト: 2)")
    print("  --marker-size <value>    平均値マーカーサイズ (デフォルト: 8)")
    print("  --mean-color '#RRGGBB'   平均値・エラーバー色 (デフォルト: #812D3A)")
    print("  --data-color '#RRGGBB'   データ点色 (デフォルト: #87CFEB)")
    print("  --stats-position <pos>   統計情報位置:")
    print("                           bottom-left, bottom-right, top-left, top-right")
    print("                           (デフォルト: bottom-left)")
    print("  --legend-position <pos>  凡例位置:")
    print("                           bottom-left, bottom-right, top-left, top-right")
    print("                           (デフォルト: bottom-left)")
    print("\nスケール設定の注意:")
    print("  - 対数スケール(log)使用時は正の値のみ有効")
    print("  - 負の値やゼロは自動的に除外されます")
    print("  - Y軸対数スケール時はエラーバーが乗法的エラーとして計算されます")
    sys.exit(1)
  
  input_file = sys.argv[1]
  
  # オプション解析
  create_stats_csv = '--no-stats-csv' not in sys.argv
  show_annotations = '--annotations' in sys.argv
  
  dpi = 300
  if '--dpi' in sys.argv:
    dpi_idx = sys.argv.index('--dpi')
    if dpi_idx + 1 < len(sys.argv):
      dpi = int(sys.argv[dpi_idx + 1])
  
  figsize = (12, 8)
  if '--size' in sys.argv:
    size_idx = sys.argv.index('--size')
    if size_idx + 2 < len(sys.argv):
      figsize = (float(sys.argv[size_idx + 1]), float(sys.argv[size_idx + 2]))
  
  # スケール設定
  xscale = 'linear'
  if '--xscale' in sys.argv:
    xscale_idx = sys.argv.index('--xscale')
    if xscale_idx + 1 < len(sys.argv):
      scale = sys.argv[xscale_idx + 1].lower()
      if validate_scale(scale):
        xscale = scale
      else:
        print(f"エラー: 無効なX軸スケール '{scale}' (linear または log を指定してください)")
        sys.exit(1)
  
  yscale = 'linear'
  if '--yscale' in sys.argv:
    yscale_idx = sys.argv.index('--yscale')
    if yscale_idx + 1 < len(sys.argv):
      scale = sys.argv[yscale_idx + 1].lower()
      if validate_scale(scale):
        yscale = scale
      else:
        print(f"エラー: 無効なY軸スケール '{scale}' (linear または log を指定してください)")
        sys.exit(1)
  
  # 線の太さ設定
  line_width = 2.0
  if '--line-width' in sys.argv:
    line_width_idx = sys.argv.index('--line-width')
    if line_width_idx + 1 < len(sys.argv):
      line_width = float(sys.argv[line_width_idx + 1])
      if line_width < 0:
        print("エラー: line-widthは0以上の値を指定してください")
        sys.exit(1)
  
  # エラーバー太さ設定
  errorbar_width = 2.0
  if '--errorbar-width' in sys.argv:
    errorbar_width_idx = sys.argv.index('--errorbar-width')
    if errorbar_width_idx + 1 < len(sys.argv):
      errorbar_width = float(sys.argv[errorbar_width_idx + 1])
      if errorbar_width < 0:
        print("エラー: errorbar-widthは0以上の値を指定してください")
        sys.exit(1)
  
  # マーカーサイズ設定
  marker_size = 8.0
  if '--marker-size' in sys.argv:
    marker_size_idx = sys.argv.index('--marker-size')
    if marker_size_idx + 1 < len(sys.argv):
      marker_size = float(sys.argv[marker_size_idx + 1])
      if marker_size <= 0:
        print("エラー: marker-sizeは正の値を指定してください")
        sys.exit(1)
  
  # 色設定
  mean_color = "#023E8A"
  if '--mean-color' in sys.argv:
    mean_color_idx = sys.argv.index('--mean-color')
    if mean_color_idx + 1 < len(sys.argv):
      color = sys.argv[mean_color_idx + 1]
      if validate_color(color):
        mean_color = color
      else:
        print(f"エラー: 無効な色形式 '{color}' (#RRGGBB形式で指定してください)")
        sys.exit(1)
  
  data_color = "#A0C4FF"
  if '--data-color' in sys.argv:
    data_color_idx = sys.argv.index('--data-color')
    if data_color_idx + 1 < len(sys.argv):
      color = sys.argv[data_color_idx + 1]
      if validate_color(color):
        data_color = color
      else:
        print(f"エラー: 無効な色形式 '{color}' (#RRGGBB形式で指定してください)")
        sys.exit(1)
  
  # 位置設定
  stats_position = "bottom-left"
  legend_position = "bottom-left"
  valid_positions = ["bottom-left", "bottom-right", "top-left", "top-right"]
  
  if '--stats-position' in sys.argv:
    stats_pos_idx = sys.argv.index('--stats-position')
    if stats_pos_idx + 1 < len(sys.argv):
      pos = sys.argv[stats_pos_idx + 1]
      if pos in valid_positions:
        stats_position = pos
      else:
        print(f"エラー: 無効な統計情報位置 '{pos}'")
        print(f"有効な位置: {', '.join(valid_positions)}")
        sys.exit(1)
  
  if '--legend-position' in sys.argv:
    legend_pos_idx = sys.argv.index('--legend-position')
    if legend_pos_idx + 1 < len(sys.argv):
      pos = sys.argv[legend_pos_idx + 1]
      if pos in valid_positions:
        legend_position = pos
      else:
        print(f"エラー: 無効な凡例位置 '{pos}'")
        print(f"有効な位置: {', '.join(valid_positions)}")
        sys.exit(1)
  
  # Y軸範囲の設定
  ylim = None
  ymin = None
  ymax = None
  
  if '--ymin' in sys.argv:
    ymin_idx = sys.argv.index('--ymin')
    if ymin_idx + 1 < len(sys.argv):
      ymin = float(sys.argv[ymin_idx + 1])
  
  if '--ymax' in sys.argv:
    ymax_idx = sys.argv.index('--ymax')
    if ymax_idx + 1 < len(sys.argv):
      ymax = float(sys.argv[ymax_idx + 1])
  
  if '--ylim' in sys.argv:
    ylim_idx = sys.argv.index('--ylim')
    if ylim_idx + 2 < len(sys.argv):
      ymin = float(sys.argv[ylim_idx + 1])
      ymax = float(sys.argv[ylim_idx + 2])
  
  if ymin is not None and ymax is not None:
    if ymin >= ymax:
      print(f"エラー: ymin ({ymin}) は ymax ({ymax}) より小さくなければなりません")
      sys.exit(1)
    # 対数スケールでの値チェック
    if yscale == 'log' and (ymin <= 0 or ymax <= 0):
      print(f"エラー: Y軸対数スケールでは正の値のみ有効です (ymin={ymin}, ymax={ymax})")
      sys.exit(1)
    ylim = (ymin, ymax)
    print(f"Y軸範囲を設定: {ymin} - {ymax}")
  elif ymin is not None or ymax is not None:
    print("警告: yminとymaxは両方指定してください。片方のみの指定は無視されます。")
  
  # X軸範囲の設定
  xlim = None
  xmin = None
  xmax = None
  
  if '--xmin' in sys.argv:
    xmin_idx = sys.argv.index('--xmin')
    if xmin_idx + 1 < len(sys.argv):
      xmin = float(sys.argv[xmin_idx + 1])
  
  if '--xmax' in sys.argv:
    xmax_idx = sys.argv.index('--xmax')
    if xmax_idx + 1 < len(sys.argv):
      xmax = float(sys.argv[xmax_idx + 1])
  
  if '--xlim' in sys.argv:
    xlim_idx = sys.argv.index('--xlim')
    if xlim_idx + 2 < len(sys.argv):
      xmin = float(sys.argv[xlim_idx + 1])
      xmax = float(sys.argv[xlim_idx + 2])
  
  if xmin is not None and xmax is not None:
    if xmin >= xmax:
      print(f"エラー: xmin ({xmin}) は xmax ({xmax}) より小さくなければなりません")
      sys.exit(1)
    # 対数スケールでの値チェック
    if xscale == 'log' and (xmin <= 0 or xmax <= 0):
      print(f"エラー: X軸対数スケールでは正の値のみ有効です (xmin={xmin}, xmax={xmax})")
      sys.exit(1)
    xlim = (xmin, xmax)
    print(f"X軸範囲を設定: {xmin} - {xmax}")
  elif xmin is not None or xmax is not None:
    print("警告: xminとxmaxは両方指定してください。片方のみの指定は無視されます。")
  
  # ラベル・タイトルの設定
  title = "Data Points with Statistical Error Bars"
  if '--title' in sys.argv:
    title_idx = sys.argv.index('--title')
    if title_idx + 1 < len(sys.argv):
      title = sys.argv[title_idx + 1]
  
  xlabel = "Distance from Latest Entry"
  if '--xlabel' in sys.argv:
    xlabel_idx = sys.argv.index('--xlabel')
    if xlabel_idx + 1 < len(sys.argv):
      xlabel = sys.argv[xlabel_idx + 1]
  
  ylabel = "Data Acquisition Time [msec]"
  if '--ylabel' in sys.argv:
    ylabel_idx = sys.argv.index('--ylabel')
    if ylabel_idx + 1 < len(sys.argv):
      ylabel = sys.argv[ylabel_idx + 1]
  
  # 処理実行
  process_csv_file(input_file, create_stats_csv, figsize, dpi, ylim, xlim, 
                  True, title, xlabel, ylabel, show_annotations,  # show_errorbars=True（常時）
                  line_width, stats_position, errorbar_width,
                  legend_position, marker_size, mean_color, data_color,
                  xscale, yscale)

if __name__ == "__main__":
  main()