from paramiko import Transport, SFTPClient
import os
import re
import yaml
import signal
import sys
import argparse
import csv
from datetime import datetime
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, DownloadColumn, TransferSpeedColumn
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table
from multiprocessing.pool import ThreadPool
from threading import Lock, Event

console = Console()
progress_lock = Lock()
shutdown_event = Event()


def signal_handler(sig, frame):
  """Ctrl+C 처리 - Graceful Shutdown"""
  if shutdown_event.is_set():
    # 두 번째 Ctrl+C - 강제 종료
    console.print("\n[bold red]✗ 강제 종료합니다...[/bold red]")
    os._exit(1)
  else:
    # 첫 번째 Ctrl+C
    console.print("\n[yellow]⚠ 종료 신호 감지... 진행 중인 다운로드를 완료하고 종료합니다.[/yellow]")
    console.print("[yellow]즉시 강제 종료하려면 Ctrl+C를 한 번 더 누르세요.[/yellow]")
    shutdown_event.set()


def load_config(config_path='config.yaml'):
  """설정 파일 로드"""
  try:
    with open(config_path, 'r', encoding='utf-8') as f:
      return yaml.safe_load(f)
  except FileNotFoundError:
    console.print(f"[red]설정 파일을 찾을 수 없습니다: {config_path}[/red]")
    console.print("[yellow]config.yaml 파일을 생성해주세요.[/yellow]")
    exit(1)
  except yaml.YAMLError as e:
    console.print(f"[red]설정 파일 파싱 오류: {e}[/red]")
    exit(1)


def connect(host, username, password):
  """SFTP 연결 (타임아웃 설정)"""
  transport = Transport((host, 22))
  transport.connect(username=username, password=password)
  transport.set_keepalive(30)
  sftp = SFTPClient.from_transport(transport)
  sftp.get_channel().settimeout(30.0)
  return sftp, transport


def filter_full_files(full_files):
  """최신 Full 파일들 필터링 - 같은 타임스탬프의 파일들만"""
  if not full_files:
    return []

  full_files.sort()
  last_full_file = full_files[-1]
  m = re.search('[0-9]{8,}', last_full_file)
  if not m:
    return []

  timestamp = m.group(0)
  return list(filter(lambda t: timestamp in t, full_files))


def filter_change_files(last_full_file, change_files):
  """Full 파일 날짜 이후의 Change 파일들 필터링 - 날짜(앞 8자리) 기준"""
  m = re.search('[0-9]{8,}', last_full_file)
  if not m:
    return []

  timestamp = m.group(0)
  cf_timestamps = []

  for t in change_files:
    m2 = re.search('[0-9]{' + str(len(timestamp)) + '}', t)
    if m2:
      cf_timestamps.append(m2.group(0))
    else:
      cf_timestamps.append("")

  filter_lst = [t[:8] >= timestamp[:8] if len(
      t) >= 8 else False for t in cf_timestamps]
  return [change_files[i] for i in range(len(change_files)) if filter_lst[i]]


def format_size(size_bytes):
  """바이트를 읽기 쉬운 형식으로 변환"""
  for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
    if size_bytes < 1024.0:
      return f"{size_bytes:.2f} {unit}"
    size_bytes /= 1024.0
  return f"{size_bytes:.2f} PB"


def save_estimate_csv(download_files_with_size):
  """파일 정보를 CSV로 저장"""
  timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
  csv_filename = f'download_estimate_{timestamp}.csv'

  total_size = sum([info['size_bytes'] for info in download_files_with_size])

  with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['directory', 'package',
                  'filename', 'size_bytes', 'size_readable']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for file_info in download_files_with_size:
      writer.writerow(file_info)

    # 마지막에 총합 추가
    writer.writerow({
        'directory': '',
        'package': '',
        'filename': 'TOTAL',
        'size_bytes': total_size,
        'size_readable': format_size(total_size)
    })

  # 터미널에 요약 테이블 표시
  table = Table(title="다운로드 예상 크기 요약")
  table.add_column("항목", style="cyan")
  table.add_column("값", style="green")

  table.add_row("총 파일 수", str(len(download_files_with_size)))
  table.add_row("총 크기", format_size(total_size))
  table.add_row("총 크기 (GB)", f"{total_size / (1024**3):.2f} GB")
  table.add_row("CSV 파일", csv_filename)

  console.print()
  console.print(table)
  console.print(f"\n[green]✓ 상세 내역이 {csv_filename}에 저장되었습니다.[/green]")


def download(file_dic, config, progress=None, overall_task_id=None):
  """파일 다운로드"""
  if shutdown_event.is_set():
    if progress and overall_task_id is not None:
      with progress_lock:
        progress.update(overall_task_id, advance=1)
    return

  file_task_id = None
  sftp = None
  transport = None

  try:
    conn = config['connection']
    host = conn['host']
    username = conn['username']
    password = conn['password']

    sftp, transport = connect(host, username, password)
    top_dir, package, file_name = file_dic

    sftp.chdir(top_dir)
    sftp.chdir(package)

    f = os.path.join(top_dir, package, file_name)

    size = sftp.stat(file_name).st_size

    if os.path.isfile(f) and size == os.path.getsize(f):
      console.print(f"[yellow]✓ {file_name} 이미 다운로드됨[/yellow]")
      sftp.close()
      transport.close()
      if progress and overall_task_id is not None:
        with progress_lock:
          progress.update(overall_task_id, advance=1)
      return

    if progress:
      with progress_lock:
        file_task_id = progress.add_task(
            f"[green]  ↳ {file_name[:50]}...",
            total=size
        )

    with open(f, 'wb') as ff:
      def _callback(transferred, total):
        if shutdown_event.is_set():
          raise KeyboardInterrupt("Download interrupted by user")

        if progress and file_task_id is not None:
          with progress_lock:
            progress.update(file_task_id, completed=transferred)

      sftp.getfo(file_name, ff, callback=_callback)

    if progress and file_task_id is not None:
      with progress_lock:
        progress.update(file_task_id, visible=False)

    console.print(f"[green]✓ {file_name} 다운로드 완료[/green]")

    if progress and overall_task_id is not None:
      with progress_lock:
        progress.update(overall_task_id, advance=1)

  except KeyboardInterrupt:
    console.print(f"[yellow]⚠ {file_name} 다운로드 중단됨[/yellow]")

    if progress and file_task_id is not None:
      with progress_lock:
        progress.update(file_task_id, visible=False)

    if progress and overall_task_id is not None:
      with progress_lock:
        progress.update(overall_task_id, advance=1)

  except Exception as e:
    console.print(f'[red]✗ 오류 발생 ({file_name}): {e}[/red]')

    if progress and file_task_id is not None:
      with progress_lock:
        progress.update(file_task_id, visible=False)

    if progress and overall_task_id is not None:
      with progress_lock:
        progress.update(overall_task_id, advance=1)

  finally:
    try:
      if sftp:
        sftp.close()
      if transport:
        transport.close()
    except:
      pass


def scan_packages(sftp, top_dir, allowed_packages, config, get_sizes=False):
  """패키지 스캔 및 다운로드 파일 목록 생성"""
  download_files = []
  download_files_with_size = []
  packages = sftp.listdir()

  file_types = config['download']['file_types']

  if allowed_packages is None:
    allowed_packages = []

  for package in packages:
    if allowed_packages and package not in allowed_packages:
      continue

    sftp.chdir(package)
    console.print(f'  → {os.path.join(top_dir, package)}')

    # dry-run이 아닐 때만 로컬 폴더 생성
    if not get_sizes:
      p = os.path.join(top_dir, package)
      if not os.path.exists(p):
        os.makedirs(p)

    files = sftp.listdir()

    # Feed Config 다운로드
    if package == 'XpressfeedFeedConfigV2' and file_types['config_files']:
      files.sort()
      if files:
        file_name = files[-1]
        download_files.append((top_dir, package, file_name))
        if get_sizes:
          try:
            size = sftp.stat(file_name).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': file_name,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass
      sftp.chdir('..')
      continue

    # 설치 파일 다운로드
    if package in ['V5Loader_Linux', 'V5Loader_Windows']:
      for f in files:
        download_files.append((top_dir, package, f))
        if get_sizes:
          try:
            size = sftp.stat(f).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': f,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass
      sftp.chdir('..')
      continue

    # Full flag 파일
    if file_types['flag_files']:
      full_flags = [f for f in files if "Full" in f and f.endswith("flg")]
      if full_flags:
        full_flags.sort()
        last_full_flag = full_flags[-1]
        download_files.append((top_dir, package, last_full_flag))
        if get_sizes:
          try:
            size = sftp.stat(last_full_flag).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': last_full_flag,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass
      else:
        console.print(f"    [dim]⚠ Full flags 없음[/dim]")

    # Full 파일
    valid_fulls = []
    if file_types['full_files']:
      full_files = [f for f in files if "Full" in f and f.endswith("zip")]
      valid_fulls = filter_full_files(full_files)
      if valid_fulls:
        console.print(f"    [cyan]→ Full 파일 {len(valid_fulls)}개 발견[/cyan]")
      for vf in valid_fulls:
        download_files.append((top_dir, package, vf))
        if get_sizes:
          try:
            size = sftp.stat(vf).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': vf,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass

    # Change 파일
    if file_types['change_files'] and valid_fulls:
      change_files = [f for f in files if "Change" in f and f.endswith("zip")]
      if change_files:
        valid_changes = filter_change_files(valid_fulls[-1], change_files)
        if valid_changes:
          console.print(
              f"    [cyan]→ Change 파일 {len(valid_changes)}개 발견[/cyan]")
        for vc in valid_changes:
          download_files.append((top_dir, package, vc))
          if get_sizes:
            try:
              size = sftp.stat(vc).st_size
              download_files_with_size.append({
                  'directory': top_dir,
                  'package': package,
                  'filename': vc,
                  'size_bytes': size,
                  'size_readable': format_size(size)
              })
            except:
              pass
      else:
        console.print(f"    [dim]⚠ Change files 없음[/dim]")

    sftp.chdir('..')

  if get_sizes:
    return download_files, download_files_with_size
  return download_files, []


def scan_xpressfeed_packages(sftp, top_dir, allowed_packages, config, get_sizes=False):
  """Xpressfeed 패키지 스캔"""
  download_files = []
  download_files_with_size = []
  packages = sftp.listdir()

  file_types = config['download']['file_types']

  if allowed_packages is None:
    allowed_packages = []

  for package in packages:
    if allowed_packages and package not in allowed_packages:
      continue

    sftp.chdir(package)
    console.print(f'  → {os.path.join(top_dir, package)}')

    # dry-run이 아닐 때만 로컬 폴더 생성
    if not get_sizes:
      p = os.path.join(top_dir, package)
      if not os.path.exists(p):
        os.makedirs(p)

    files = sftp.listdir()

    if package in ['suppcxf']:
      for lf in files:
        download_files.append((top_dir, package, lf))
        if get_sizes:
          try:
            size = sftp.stat(lf).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': lf,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass
      sftp.chdir('..')
      continue

    # Full flag 파일
    if file_types['flag_files']:
      full_flags = [f for f in files if f.startswith(
          "f_") and f.endswith("flg")]
      if full_flags:
        full_flags.sort()
        last_full_flag = full_flags[-1]
        download_files.append((top_dir, package, last_full_flag))
        if get_sizes:
          try:
            size = sftp.stat(last_full_flag).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': last_full_flag,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass
      else:
        console.print(f"    [dim]⚠ Full flags 없음[/dim]")

    # Full 파일
    valid_fulls = []
    if file_types['full_files']:
      full_files = [f for f in files if f.startswith(
          "f_") and f.endswith("zip")]
      valid_fulls = filter_full_files(full_files)
      if valid_fulls:
        console.print(f"    [cyan]→ Full 파일 {len(valid_fulls)}개 발견[/cyan]")
      for vf in valid_fulls:
        download_files.append((top_dir, package, vf))
        if get_sizes:
          try:
            size = sftp.stat(vf).st_size
            download_files_with_size.append({
                'directory': top_dir,
                'package': package,
                'filename': vf,
                'size_bytes': size,
                'size_readable': format_size(size)
            })
          except:
            pass

    # Change 파일
    if file_types['change_files'] and valid_fulls:
      change_files = [f for f in files if f.startswith("t_")]
      if change_files:
        valid_changes = filter_change_files(valid_fulls[-1], change_files)
        if valid_changes:
          console.print(
              f"    [cyan]→ Change 파일 {len(valid_changes)}개 발견[/cyan]")
        for vc in valid_changes:
          download_files.append((top_dir, package, vc))
          if get_sizes:
            try:
              size = sftp.stat(vc).st_size
              download_files_with_size.append({
                  'directory': top_dir,
                  'package': package,
                  'filename': vc,
                  'size_bytes': size,
                  'size_readable': format_size(size)
              })
            except:
              pass
      else:
        console.print(f"    [dim]⚠ Change files 없음[/dim]")

    sftp.chdir('..')

  if get_sizes:
    return download_files, download_files_with_size
  return download_files, []


def main():
  """메인 함수"""
  parser = argparse.ArgumentParser(
      description='S&P Global Xpressfeed Downloader',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog="""
예제:
  python xf-postbox.py                # 일반 다운로드
  python xf-postbox.py --dry-run      # 크기만 확인 (CSV 저장)
        """
  )
  parser.add_argument(
      '--dry-run',
      action='store_true',
      help='파일 크기만 확인하고 다운로드는 하지 않음 (CSV로 저장)'
  )
  args = parser.parse_args()

  signal.signal(signal.SIGINT, signal_handler)

  console.print("[bold blue]S&P Global Xpressfeed Downloader[/bold blue]")
  console.print("=" * 50)

  if args.dry_run:
    console.print("[yellow]⚠ Dry-run 모드 활성화[/yellow]")

  config = load_config()
  console.print("[green]✓ 설정 파일 로드 완료[/green]")

  try:
    conn = config['connection']
    host = conn['host']
    username = conn['username']
    password = conn['password']
    destination = conn['destination']

    if not all([host, username, password, destination]):
      console.print("[red]config.yaml의 connection 설정을 확인하세요[/red]")
      exit(1)

  except KeyError as e:
    console.print(f"[red]config.yaml에 필수 항목이 없습니다: {e}[/red]")
    exit(1)

  # 로컬 디렉토리 설정 (dry-run이 아닐 때만)
  if not args.dry_run:
    if not os.path.exists(destination):
      os.makedirs(destination)
      console.print(f"[cyan]대상 디렉토리 생성: {destination}[/cyan]")

    os.chdir(destination)
    for dir_name in config['directories']:
      if not os.path.exists(dir_name):
        os.mkdir(dir_name)
        console.print(f"[cyan]하위 디렉토리 생성: {dir_name}[/cyan]")

  # SFTP 연결
  console.print(f"\n[cyan]SFTP 서버 연결 중... ({host})[/cyan]")
  try:
    sftp, transport = connect(host, username, password)
    console.print("[green]✓ SFTP 연결 성공[/green]\n")
  except Exception as e:
    console.print(f"[red]✗ SFTP 연결 실패: {e}[/red]")
    exit(1)

  download_files = []
  download_files_with_size = []

  # Products 디렉토리 스캔
  if 'Products' in sftp.listdir('.'):
    sftp.chdir('Products')
    console.print('[bold]Products 디렉토리 스캔 중...[/bold]')
    products_files, products_sizes = scan_packages(
        sftp, 'Products',
        config['packages'].get('products', []),
        config,
        get_sizes=args.dry_run
    )
    download_files.extend(products_files)
    download_files_with_size.extend(products_sizes)
    console.print(f"[cyan]→ Products에서 {len(products_files)}개 파일 발견[/cyan]")
    sftp.chdir('..')

  # Xpressfeed 디렉토리 스캔
  if 'Xpressfeed' in sftp.listdir('.'):
    sftp.chdir('Xpressfeed')
    console.print('\n[bold]Xpressfeed 디렉토리 스캔 중...[/bold]')
    xpressfeed_files, xpressfeed_sizes = scan_xpressfeed_packages(
        sftp, 'Xpressfeed',
        config['packages'].get('xpressfeed', []),
        config,
        get_sizes=args.dry_run
    )
    download_files.extend(xpressfeed_files)
    download_files_with_size.extend(xpressfeed_sizes)
    console.print(
        f"[cyan]→ Xpressfeed에서 {len(xpressfeed_files)}개 파일 발견[/cyan]")
    sftp.chdir('..')

  sftp.close()
  transport.close()

  console.print(
      f"\n[bold green]파일 스캔 완료: 총 {len(download_files)}개 파일 발견[/bold green]\n")

  if not download_files:
    console.print("[yellow]다운로드할 파일이 없습니다.[/yellow]")
    return

  # Dry-run 모드
  if args.dry_run:
    save_estimate_csv(download_files_with_size)
    return

  # 다운로드 시작 확인
  if not Confirm.ask(f"[bold]{len(download_files)}개 파일을 다운로드하시겠습니까?[/bold]", default=True):
    console.print("[yellow]다운로드를 취소했습니다.[/yellow]")
    return

  # 파일 다운로드
  with Progress(
      SpinnerColumn(),
      TextColumn("[bold blue]{task.description}"),
      BarColumn(complete_style="green", finished_style="bold green"),
      DownloadColumn(),
      TransferSpeedColumn(),
      TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
      TimeRemainingColumn(),
      console=console,
      transient=False,
      expand=True
  ) as progress:

    overall_task = progress.add_task(
        f"[cyan]전체 진행률 (0/{len(download_files)} files)",
        total=len(download_files)
    )

    def download_wrapper(file_dic):
      if not shutdown_event.is_set():
        download(file_dic, config, progress, overall_task)

    thread_count = config['download'].get('thread_count')
    if thread_count is None:
      thread_count = max(1, os.cpu_count() - 1 if os.cpu_count() else 4)

    console.print(f"[cyan]병렬 다운로드 스레드 수: {thread_count}[/cyan]\n")

    thread_pool = ThreadPool(thread_count)

    try:
      result = thread_pool.map_async(download_wrapper, download_files)
      while not result.ready():
        result.wait(0.1)
        with progress_lock:
          completed = progress.tasks[overall_task].completed
          total = progress.tasks[overall_task].total
          progress.update(
              overall_task,
              description=f"[cyan]전체 진행률 ({int(completed)}/{int(total)} files)"
          )

    except KeyboardInterrupt:
      console.print("\n[yellow]⚠ 종료 중...[/yellow]")
      shutdown_event.set()
    finally:
      thread_pool.close()
      thread_pool.join(timeout=5)
      console.print("[dim]스레드 정리 완료[/dim]")

  if shutdown_event.is_set():
    console.print("\n[yellow]⚠ 다운로드가 사용자에 의해 중단되었습니다.[/yellow]")
  else:
    console.print("\n[bold green]✓ 모든 파일 다운로드 완료![/bold green]")


if __name__ == "__main__":
  main()
