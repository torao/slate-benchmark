#!/usr/bin/env python3
"""
ベンチマーク実行環境情報収集スクリプト
Usage: python3 benchmark_env_info.py [target_directory]
"""

import os
import sys
import subprocess
import json
import re
import time
import tempfile
import platform
from datetime import datetime
from pathlib import Path


def run_command(cmd, shell=False):
  """コマンドを実行して結果を返す"""
  try:
    if shell:
      result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
    else:
      result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.stdout.strip() if result.returncode == 0 else None
  except (subprocess.TimeoutExpired, subprocess.SubprocessError):
    return None


def get_storage_info(target_dir):
  """ストレージ情報を取得"""
  info = {}
  
  # デバイス情報を取得
  df_output = run_command(['df', target_dir])
  if df_output:
    lines = df_output.split('\n')
    if len(lines) > 1:
      device = lines[1].split()[0]
      info['device'] = device
      
      # ファイルシステム情報を取得
      df_t_output = run_command(['df', '-T', target_dir])
      if df_t_output:
        df_lines = df_t_output.split('\n')
        if len(df_lines) > 1:
          fields = df_lines[1].split()
          if len(fields) >= 2:
            info['filesystem'] = fields[1]
      
      # マウント情報から詳細を取得
      mount_output = run_command(['mount'])
      if mount_output:
        for line in mount_output.split('\n'):
          if device in line:
            # マウント行の例: /dev/sda1 on / type ext4 (rw,relatime)
            parts = line.split()
            for i, part in enumerate(parts):
              if part == 'type' and i + 1 < len(parts):
                fs_type = parts[i + 1]
                info['filesystem'] = fs_type
                break
              # マウントオプションを取得
              if '(' in part and ')' in part:
                mount_options = part.strip('()')
                info['mount_options'] = mount_options
            break
      
      # findmntコマンドでより詳細な情報を取得
      findmnt_output = run_command(['findmnt', '-n', '-o', 'FSTYPE,OPTIONS', target_dir])
      if findmnt_output:
        parts = findmnt_output.strip().split(None, 1)
        if len(parts) >= 1:
          info['filesystem'] = parts[0]
        if len(parts) >= 2:
          info['mount_options'] = parts[1]
      
      # ファイルシステムの使用量情報
      df_h_output = run_command(['df', '-h', target_dir])
      if df_h_output:
        df_lines = df_h_output.split('\n')
        if len(df_lines) > 1:
          fields = df_lines[1].split()
          if len(fields) >= 6:
            info['total_size'] = fields[1]
            info['used_size'] = fields[2]
            info['available_size'] = fields[3]
            info['usage_percent'] = fields[4]
      
      # inodeの使用量情報
      df_i_output = run_command(['df', '-i', target_dir])
      if df_i_output:
        df_lines = df_i_output.split('\n')
        if len(df_lines) > 1:
          fields = df_lines[1].split()
          if len(fields) >= 6:
            info['total_inodes'] = fields[1]
            info['used_inodes'] = fields[2]
            info['available_inodes'] = fields[3]
            info['inode_usage_percent'] = fields[4]
      
      # デバイス名からブロックデバイス名を推定
      if device.startswith('/dev/'):
        # パーティション番号を除去してベースデバイスを取得
        base_device = re.sub(r'[0-9]+$', '', device.split('/')[-1])
        
        # lsblkでデバイス情報を取得
        lsblk_output = run_command(['lsblk', '-J', '-o', 'NAME,TYPE,SIZE,ROTA,MODEL,SERIAL,FSTYPE'])
        if lsblk_output:
          try:
            lsblk_data = json.loads(lsblk_output)
            for dev in lsblk_data.get('blockdevices', []):
              if dev['name'] == base_device:
                info['model'] = dev.get('model', 'Unknown')
                info['serial'] = dev.get('serial', 'Unknown')
                info['size'] = dev.get('size', 'Unknown')
                info['rotational'] = dev.get('rota', '0') == '1'
                info['type'] = 'HDD' if info['rotational'] else 'SSD/NVMe'
                break
              # パーティションの場合は子要素もチェック
              for child in dev.get('children', []):
                if child.get('name', '').startswith(base_device) or child.get('name') == device.split('/')[-1]:
                  if not info.get('filesystem') and child.get('fstype'):
                    info['filesystem'] = child.get('fstype')
                  # 親デバイスの情報を使用
                  if not info.get('model'):
                    info['model'] = dev.get('model', 'Unknown')
                  if not info.get('serial'):
                    info['serial'] = dev.get('serial', 'Unknown')
                  if not info.get('size'):
                    info['size'] = dev.get('size', 'Unknown')
                  if 'rotational' not in info:
                    info['rotational'] = dev.get('rota', '0') == '1'
                    info['type'] = 'HDD' if info['rotational'] else 'SSD/NVMe'
                  break
          except json.JSONDecodeError:
            pass
        
        # smartctlで詳細情報を取得（あれば）
        smartctl_output = run_command(['smartctl', '-i', f'/dev/{base_device}'], shell=False)
        if smartctl_output:
          for line in smartctl_output.split('\n'):
            if 'Device Model:' in line or 'Model Number:' in line:
              info['detailed_model'] = line.split(':', 1)[1].strip()
            elif 'Serial Number:' in line:
              info['detailed_serial'] = line.split(':', 1)[1].strip()
            elif 'Form Factor:' in line:
              info['form_factor'] = line.split(':', 1)[1].strip()
      
      # ファイルシステム固有の情報を取得
      if info.get('filesystem'):
        fs_type = info['filesystem']
        
        # ext系ファイルシステムの場合
        if fs_type.startswith('ext'):
          tune2fs_output = run_command(['tune2fs', '-l', device])
          if tune2fs_output:
            for line in tune2fs_output.split('\n'):
              if 'Block size:' in line:
                info['block_size'] = line.split(':')[1].strip()
              elif 'Fragment size:' in line:
                info['fragment_size'] = line.split(':')[1].strip()
              elif 'Filesystem features:' in line:
                info['fs_features'] = line.split(':')[1].strip()
        
        # XFSファイルシステムの場合
        elif fs_type == 'xfs':
          xfs_info_output = run_command(['xfs_info', target_dir])
          if xfs_info_output:
            for line in xfs_info_output.split('\n'):
              if 'bsize=' in line:
                block_match = re.search(r'bsize=(\d+)', line)
                if block_match:
                  info['block_size'] = f"{block_match.group(1)} bytes"
        
        # Btrfsファイルシステムの場合
        elif fs_type == 'btrfs':
          btrfs_show_output = run_command(['btrfs', 'filesystem', 'show', target_dir])
          if btrfs_show_output:
            info['btrfs_info'] = 'Available'
  
  return info


def measure_io_performance(target_dir):
  """I/O性能を測定"""
  perf_info = {}
  
  try:
    test_file = os.path.join(target_dir, 'benchmark_io_test.tmp')
    
    # 書き込み速度測定（1GBファイル）
    print("Measuring write performance...")
    write_cmd = f"dd if=/dev/zero of={test_file} bs=1M count=1024 oflag=direct 2>&1"
    write_output = run_command(write_cmd, shell=True)
    
    if write_output:
      # ddの出力から速度を抽出
      speed_match = re.search(r'(\d+(?:\.\d+)?)\s*([KMGT]?B)/s', write_output)
      if speed_match:
        speed_value = float(speed_match.group(1))
        speed_unit = speed_match.group(2)
        perf_info['write_speed'] = f"{speed_value} {speed_unit}/s"
    
    # ファイルが作成されていることを確認
    if os.path.exists(test_file):
      # 読み込み速度測定
      print("Measuring read performance...")
      read_cmd = f"dd if={test_file} of=/dev/null bs=1M iflag=direct 2>&1"
      read_output = run_command(read_cmd, shell=True)
      
      if read_output:
        speed_match = re.search(r'(\d+(?:\.\d+)?)\s*([KMGT]?B)/s', read_output)
        if speed_match:
          speed_value = float(speed_match.group(1))
          speed_unit = speed_match.group(2)
          perf_info['read_speed'] = f"{speed_value} {speed_unit}/s"
      
      # テストファイルを削除
      os.remove(test_file)
    
    # ランダムI/O測定（fioがあれば）
    fio_output = run_command(['fio', '--version'])
    if fio_output:
      print("Measuring random I/O performance with fio...")
      fio_cmd = f"""fio --name=random-rw --ioengine=libaio --iodepth=4 --rw=randrw --bs=4k --direct=1 --size=100M --numjobs=1 --runtime=30 --group_reporting --filename={target_dir}/fio_test.tmp --output-format=json"""
      fio_result = run_command(fio_cmd, shell=True)
      
      if fio_result:
        try:
          fio_data = json.loads(fio_result)
          if 'jobs' in fio_data and len(fio_data['jobs']) > 0:
            job = fio_data['jobs'][0]
            read_iops = job.get('read', {}).get('iops', 0)
            write_iops = job.get('write', {}).get('iops', 0)
            perf_info['random_read_iops'] = f"{read_iops:.0f} IOPS"
            perf_info['random_write_iops'] = f"{write_iops:.0f} IOPS"
        except (json.JSONDecodeError, KeyError):
          pass
      
      # fioテストファイルを削除
      fio_test_file = os.path.join(target_dir, 'fio_test.tmp')
      if os.path.exists(fio_test_file):
        os.remove(fio_test_file)
  
  except Exception as e:
    perf_info['error'] = f"I/O performance measurement failed: {str(e)}"
  
  return perf_info


def get_cpu_info():
  """CPU情報を取得"""
  info = {}
  
  # /proc/cpuinfoから情報を取得
  try:
    with open('/proc/cpuinfo', 'r') as f:
      cpuinfo = f.read()
    
    # CPU名を取得
    model_match = re.search(r'model name\s*:\s*(.+)', cpuinfo)
    if model_match:
      info['model'] = model_match.group(1).strip()
    
    # 物理コア数とスレッド数を取得
    core_ids = set()
    thread_count = 0
    
    for line in cpuinfo.split('\n'):
      if line.startswith('core id'):
        core_ids.add(line.split(':')[1].strip())
      elif line.startswith('processor'):
        thread_count += 1
    
    info['physical_cores'] = len(core_ids) if core_ids else thread_count
    info['threads'] = thread_count
    
    # 周波数情報
    freq_match = re.search(r'cpu MHz\s*:\s*(\d+(?:\.\d+)?)', cpuinfo)
    if freq_match:
      info['frequency_mhz'] = float(freq_match.group(1))
  
  except FileNotFoundError:
    pass
  
  # lscpuコマンドからも情報を取得
  lscpu_output = run_command(['lscpu'])
  if lscpu_output:
    for line in lscpu_output.split('\n'):
      if 'Model name:' in line:
        info['model'] = line.split(':', 1)[1].strip()
      elif 'CPU(s):' in line and 'NUMA' not in line:
        info['threads'] = int(line.split(':')[1].strip())
      elif 'Core(s) per socket:' in line:
        cores_per_socket = int(line.split(':')[1].strip())
        socket_match = re.search(r'Socket\(s\):\s*(\d+)', lscpu_output)
        if socket_match:
          sockets = int(socket_match.group(1))
          info['physical_cores'] = cores_per_socket * sockets
  
  return info


def get_memory_info():
  """メモリ情報を取得"""
  info = {}
  
  try:
    with open('/proc/meminfo', 'r') as f:
      meminfo = f.read()
    
    # 総メモリ量
    total_match = re.search(r'MemTotal:\s*(\d+)\s*kB', meminfo)
    if total_match:
      total_kb = int(total_match.group(1))
      info['total_gb'] = round(total_kb / (1024 * 1024), 1)
    
    # 利用可能メモリ
    available_match = re.search(r'MemAvailable:\s*(\d+)\s*kB', meminfo)
    if available_match:
      available_kb = int(available_match.group(1))
      info['available_gb'] = round(available_kb / (1024 * 1024), 1)
  
  except FileNotFoundError:
    pass
  
  return info


def get_system_info():
  """システム情報を取得"""
  info = {}
  
  # OS情報
  try:
    with open('/etc/os-release', 'r') as f:
      os_release = f.read()
    
    name_match = re.search(r'PRETTY_NAME="([^"]+)"', os_release)
    if name_match:
      info['os'] = name_match.group(1)
  except FileNotFoundError:
    info['os'] = platform.platform()
  
  # カーネルバージョン
  info['kernel'] = platform.release()
  
  # アーキテクチャ
  info['architecture'] = platform.machine()
  
  # 起動時間
  uptime_output = run_command(['uptime', '-p'])
  if uptime_output:
    info['uptime'] = uptime_output.replace('up ', '')
  
  return info


def get_public_ip_info():
  """パブリックIP情報を取得"""
  ip_info = {}
  
  # パブリックIPアドレスを取得
  services = [
    'https://api.ipify.org',
    'https://ipinfo.io/ip',
    'https://icanhazip.com',
    'https://ifconfig.me/ip'
  ]
  
  public_ip = None
  for service in services:
    try:
      result = run_command(['curl', '-s', '--max-time', '10', service])
      if result and result.strip():
        public_ip = result.strip()
        break
    except:
      continue
  
  if public_ip:
    ip_info['public_ip'] = public_ip
    
    # IPアドレスから詳細情報を取得
    try:
      # ipinfo.ioから詳細情報を取得
      ipinfo_result = run_command(['curl', '-s', '--max-time', '10', f'https://ipinfo.io/{public_ip}/json'])
      if ipinfo_result:
        try:
          ipinfo_data = json.loads(ipinfo_result)
          ip_info['org'] = ipinfo_data.get('org', '')
          ip_info['city'] = ipinfo_data.get('city', '')
          ip_info['region'] = ipinfo_data.get('region', '')
          ip_info['country'] = ipinfo_data.get('country', '')
          ip_info['location'] = ipinfo_data.get('loc', '')
          
          # クラウドプロバイダーを組織名から推定
          org_lower = ip_info['org'].lower()
          if 'amazon' in org_lower or 'aws' in org_lower or 'ec2' in org_lower:
            ip_info['detected_cloud_provider'] = 'AWS'
          elif 'google' in org_lower or 'gcp' in org_lower:
            ip_info['detected_cloud_provider'] = 'Google Cloud'
          elif 'microsoft' in org_lower or 'azure' in org_lower:
            ip_info['detected_cloud_provider'] = 'Microsoft Azure'
          elif 'digitalocean' in org_lower:
            ip_info['detected_cloud_provider'] = 'DigitalOcean'
          elif 'linode' in org_lower:
            ip_info['detected_cloud_provider'] = 'Linode'
          elif 'vultr' in org_lower:
            ip_info['detected_cloud_provider'] = 'Vultr'
          
        except json.JSONDecodeError:
          pass
    except:
      pass
  
  return ip_info


def detect_cloud_from_network():
  """ネットワーク設定からクラウドプロバイダーを検出"""
  cloud_hints = {}
  
  # DNSサーバーをチェック
  try:
    with open('/etc/resolv.conf', 'r') as f:
      resolv_content = f.read()
    
    dns_servers = []
    for line in resolv_content.split('\n'):
      if line.startswith('nameserver'):
        dns_ip = line.split()[1]
        dns_servers.append(dns_ip)
    
    cloud_hints['dns_servers'] = dns_servers
    
    # AWS DNS (169.254.169.253)
    if '169.254.169.253' in dns_servers:
      cloud_hints['dns_provider'] = 'AWS'
    # Google DNS
    elif '8.8.8.8' in dns_servers or '8.8.4.4' in dns_servers:
      cloud_hints['dns_provider'] = 'Google (Public)'
    # Cloudflare DNS
    elif '1.1.1.1' in dns_servers or '1.0.0.1' in dns_servers:
      cloud_hints['dns_provider'] = 'Cloudflare'
  except:
    pass
  
  # ネットワークインターフェースをチェック
  try:
    # ネットワークインターフェース名から推定
    interfaces = run_command(['ip', 'link', 'show'])
    if interfaces:
      if 'ens' in interfaces:  # AWS EC2の典型的なインターフェース名
        cloud_hints['network_interface_hint'] = 'AWS-like (ens*)'
      elif 'eth0' in interfaces:
        cloud_hints['network_interface_hint'] = 'Traditional (eth0)'
  except:
    pass
  
  return cloud_hints


def get_cloud_instance_info():
  """クラウドインスタンス情報を取得"""
  info = {}
  
  # AWSメタデータ
  try:
    # インスタンスタイプ
    instance_type = run_command(['curl', '-s', '--max-time', '5', 'http://169.254.169.254/latest/meta-data/instance-type'])
    if instance_type:
      info['aws_instance_type'] = instance_type
      info['detected_cloud_provider'] = 'AWS'
    
    # インスタンスID
    instance_id = run_command(['curl', '-s', '--max-time', '5', 'http://169.254.169.254/latest/meta-data/instance-id'])
    if instance_id:
      info['aws_instance_id'] = instance_id
    
    # リージョン
    az = run_command(['curl', '-s', '--max-time', '5', 'http://169.254.169.254/latest/meta-data/placement/availability-zone'])
    if az:
      info['aws_availability_zone'] = az
      info['aws_region'] = az[:-1]  # 最後の文字(a,b,c等)を除去
    
    # プライベートIPアドレス
    private_ip = run_command(['curl', '-s', '--max-time', '5', 'http://169.254.169.254/latest/meta-data/local-ipv4'])
    if private_ip:
      info['aws_private_ip'] = private_ip
    
    # パブリックIPアドレス
    public_ip = run_command(['curl', '-s', '--max-time', '5', 'http://169.254.169.254/latest/meta-data/public-ipv4'])
    if public_ip:
      info['aws_public_ip'] = public_ip
      
  except:
    pass
  
  # GCPメタデータ
  try:
    machine_type = run_command(['curl', '-s', '--max-time', '5', '-H', 'Metadata-Flavor: Google', 'http://metadata.google.internal/computeMetadata/v1/instance/machine-type'])
    if machine_type:
      info['gcp_machine_type'] = machine_type.split('/')[-1]
      info['detected_cloud_provider'] = 'Google Cloud'
    
    # GCPゾーン
    zone = run_command(['curl', '-s', '--max-time', '5', '-H', 'Metadata-Flavor: Google', 'http://metadata.google.internal/computeMetadata/v1/instance/zone'])
    if zone:
      info['gcp_zone'] = zone.split('/')[-1]
    
    # GCPプロジェクトID
    project = run_command(['curl', '-s', '--max-time', '5', '-H', 'Metadata-Flavor: Google', 'http://metadata.google.internal/computeMetadata/v1/project/project-id'])
    if project:
      info['gcp_project_id'] = project
      
  except:
    pass
  
  # Azureメタデータ
  try:
    azure_metadata = run_command(['curl', '-s', '--max-time', '5', '-H', 'Metadata:true', 'http://169.254.169.254/metadata/instance?api-version=2021-02-01'])
    if azure_metadata:
      try:
        azure_data = json.loads(azure_metadata)
        compute = azure_data.get('compute', {})
        info['azure_vm_size'] = compute.get('vmSize', '')
        info['azure_location'] = compute.get('location', '')
        info['azure_resource_group'] = compute.get('resourceGroupName', '')
        info['detected_cloud_provider'] = 'Microsoft Azure'
      except json.JSONDecodeError:
        pass
  except:
    pass
  
  # Digital Oceanメタデータ
  try:
    do_metadata = run_command(['curl', '-s', '--max-time', '5', 'http://169.254.169.254/metadata/v1.json'])
    if do_metadata:
      try:
        do_data = json.loads(do_metadata)
        info['do_droplet_id'] = do_data.get('droplet_id', '')
        info['do_region'] = do_data.get('region', '')
        info['detected_cloud_provider'] = 'DigitalOcean'
      except json.JSONDecodeError:
        pass
  except:
    pass
  
  return info


def create_markdown_report(target_dir, storage_info, io_perf, cpu_info, memory_info, system_info, cloud_info, ip_info, network_hints):
  """Markdownレポートを生成"""
  timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
  filename = f"{timestamp}-benchmark-environment.md"
  
  # resultsディレクトリを作成
  results_dir = Path('results')
  results_dir.mkdir(exist_ok=True)
  
  filepath = results_dir / filename
  
  with open(filepath, 'w', encoding='utf-8') as f:
    f.write(f"# Benchmark Environment Report\n\n")
    f.write(f"- **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"- **Target Directory:** `{target_dir}`\n\n")
    
    # システム情報
    f.write("## System Information\n\n")
    if system_info.get('os'):
      f.write(f"- **OS:** {system_info['os']}\n")
    if system_info.get('kernel'):
      f.write(f"- **Kernel:** {system_info['kernel']}\n")
    if system_info.get('architecture'):
      f.write(f"- **Architecture:** {system_info['architecture']}\n")
    if system_info.get('uptime'):
      f.write(f"- **Uptime:** {system_info['uptime']}\n")
    f.write("\n")
    
    # クラウドプロバイダー検出結果
    detected_providers = set()
    if cloud_info.get('detected_cloud_provider'):
      detected_providers.add(cloud_info['detected_cloud_provider'])
    if ip_info.get('detected_cloud_provider'):
      detected_providers.add(ip_info['detected_cloud_provider'])
    
    if detected_providers or cloud_info or ip_info or network_hints:
      f.write("## Cloud Provider Detection\n\n")
      
      if detected_providers:
        f.write(f"- **Detected Provider(s):** {', '.join(detected_providers)}\n")
      
      # IP情報
      if ip_info.get('public_ip'):
        f.write(f"- **Public IP:** {ip_info['public_ip']}\n")
      if ip_info.get('org'):
        f.write(f"- **IP Organization:** {ip_info['org']}\n")
      if ip_info.get('city') and ip_info.get('region') and ip_info.get('country'):
        f.write(f"- **Location:** {ip_info['city']}, {ip_info['region']}, {ip_info['country']}\n")
      
      # ネットワークヒント
      if network_hints.get('dns_provider'):
        f.write(f"- **DNS Provider:** {network_hints['dns_provider']}\n")
      if network_hints.get('network_interface_hint'):
        f.write(f"- **Network Interface:** {network_hints['network_interface_hint']}\n")
      
      f.write("\n")
    
    # 具体的なクラウドインスタンス情報
    if cloud_info:
      f.write("## Cloud Instance Information\n\n")
      for key, value in cloud_info.items():
        if key != 'detected_cloud_provider':  # 重複を避ける
          formatted_key = key.replace('_', ' ').title()
          f.write(f"- **{formatted_key}:** {value}\n")
      f.write("\n")
    
    # CPU情報
    f.write("## CPU Information\n\n")
    if cpu_info.get('model'):
      f.write(f"- **Model:** {cpu_info['model']}\n")
    if cpu_info.get('physical_cores'):
      f.write(f"- **Physical Cores:** {cpu_info['physical_cores']}\n")
    if cpu_info.get('threads'):
      f.write(f"- **Threads:** {cpu_info['threads']}\n")
    if cpu_info.get('frequency_mhz'):
      f.write(f"- **Base Frequency:** {cpu_info['frequency_mhz']:.0f} MHz\n")
    f.write("\n")
    
    # メモリ情報
    f.write("## Memory Information\n\n")
    if memory_info.get('total_gb'):
      f.write(f"- **Total Memory:** {memory_info['total_gb']} GB\n")
    if memory_info.get('available_gb'):
      f.write(f"- **Available Memory:** {memory_info['available_gb']} GB\n")
    f.write("\n")
    
    # ストレージ情報
    f.write("## Storage Information\n\n")
    if storage_info.get('device'):
      f.write(f"- **Device:** {storage_info['device']}\n")
    if storage_info.get('filesystem'):
      f.write(f"- **Filesystem:** {storage_info['filesystem']}\n")
    if storage_info.get('type'):
      f.write(f"- **Storage Type:** {storage_info['type']}\n")
    if storage_info.get('size'):
      f.write(f"- **Device Size:** {storage_info['size']}\n")
    if storage_info.get('total_size'):
      f.write(f"- **Total Space:** {storage_info['total_size']}\n")
    if storage_info.get('used_size'):
      f.write(f"- **Used Space:** {storage_info['used_size']}\n")
    if storage_info.get('available_size'):
      f.write(f"- **Available Space:** {storage_info['available_size']}\n")
    if storage_info.get('usage_percent'):
      f.write(f"- **Usage:** {storage_info['usage_percent']}\n")
    if storage_info.get('mount_options'):
      f.write(f"- **Mount Options:** {storage_info['mount_options']}\n")
    if storage_info.get('model'):
      f.write(f"- **Model:** {storage_info['model']}\n")
    if storage_info.get('detailed_model'):
      f.write(f"- **Detailed Model:** {storage_info['detailed_model']}\n")
    if storage_info.get('serial'):
      f.write(f"- **Serial:** {storage_info['serial']}\n")
    if storage_info.get('form_factor'):
      f.write(f"- **Form Factor:** {storage_info['form_factor']}\n")
    if storage_info.get('block_size'):
      f.write(f"- **Block Size:** {storage_info['block_size']}\n")
    if storage_info.get('fs_features'):
      f.write(f"- **Filesystem Features:** {storage_info['fs_features']}\n")
    
    # inode情報（利用可能な場合）
    if storage_info.get('total_inodes'):
      f.write(f"- **Total Inodes:** {storage_info['total_inodes']}\n")
    if storage_info.get('used_inodes'):
      f.write(f"- **Used Inodes:** {storage_info['used_inodes']}\n")
    if storage_info.get('inode_usage_percent'):
      f.write(f"- **Inode Usage:** {storage_info['inode_usage_percent']}\n")
    f.write("\n")
    
    # I/O性能
    f.write("## I/O Performance\n\n")
    if io_perf.get('write_speed'):
      f.write(f"- **Sequential Write Speed:** {io_perf['write_speed']}\n")
    if io_perf.get('read_speed'):
      f.write(f"- **Sequential Read Speed:** {io_perf['read_speed']}\n")
    if io_perf.get('random_read_iops'):
      f.write(f"- **Random Read IOPS:** {io_perf['random_read_iops']}\n")
    if io_perf.get('random_write_iops'):
      f.write(f"- **Random Write IOPS:** {io_perf['random_write_iops']}\n")
    if io_perf.get('error'):
      f.write(f"- **Error:** {io_perf['error']}\n")
    f.write("\n")
    
    # 詳細なネットワーク情報
    if network_hints.get('dns_servers'):
      f.write("## Network Configuration\n\n")
      f.write(f"- **DNS Servers:** {', '.join(network_hints['dns_servers'])}\n")
      f.write("\n")
    
    # 注意事項
    f.write("## Notes\n\n")
    f.write("- Cloud provider detection is based on metadata services and IP geolocation\n")
    f.write("- I/O performance measurements use direct I/O to bypass system cache\n")
    f.write("- Random I/O measurements require `fio` tool to be installed\n")
    f.write("- Some storage details require `smartctl` tool for complete information\n")
  
  return filepath


def main():
  target_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
  target_dir = os.path.abspath(target_dir)
  
  if not os.path.exists(target_dir):
    print(f"Error: Directory '{target_dir}' does not exist.")
    sys.exit(1)
  
  print(f"Collecting environment information for: {target_dir}")
  print("This may take a few minutes due to I/O performance measurements...")
  
  # 各種情報を収集
  print("Collecting storage information...")
  storage_info = get_storage_info(target_dir)
  
  print("Measuring I/O performance...")
  io_perf = measure_io_performance(target_dir)
  
  print("Collecting CPU information...")
  cpu_info = get_cpu_info()
  
  print("Collecting memory information...")
  memory_info = get_memory_info()
  
  print("Collecting system information...")
  system_info = get_system_info()
  
  print("Detecting cloud provider...")
  cloud_info = get_cloud_instance_info()
  
  print("Collecting public IP information...")
  ip_info = get_public_ip_info()
  
  print("Analyzing network configuration...")
  network_hints = detect_cloud_from_network()
  
  print("Generating report...")
  report_path = create_markdown_report(
    target_dir, storage_info, io_perf, cpu_info, 
    memory_info, system_info, cloud_info, ip_info, network_hints
  )
  
  print(f"Report generated: {report_path}")
  
  # 検出されたクラウドプロバイダーを表示
  detected_providers = set()
  if cloud_info.get('detected_cloud_provider'):
    detected_providers.add(cloud_info['detected_cloud_provider'])
  if ip_info.get('detected_cloud_provider'):
    detected_providers.add(ip_info['detected_cloud_provider'])
  
  if detected_providers:
    print(f"Detected cloud provider(s): {', '.join(detected_providers)}")
  else:
    print("Cloud provider detection: No cloud environment detected")
  
  print("Done!")


if __name__ == "__main__":
  main()