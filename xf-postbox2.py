from paramiko import Transport, SFTPClient
from decouple import config
import os
import re
from tqdm import tqdm
from multiprocessing.pool import ThreadPool

def connect(host, username, password):
  transport = Transport((host, 22))
  transport.connect(username=username, password=password)
  sftp = SFTPClient.from_transport(transport)
  return sftp, transport

def filter_full_files(full_files):
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
  m = re.search('[0-9]{8,}', last_full_file)
  if not m:
    return []
  timestamp = m.group(0)
  cf_timestamps = []
  for t in change_files:
    m2 = re.search('[0-9]{' + str(len(timestamp)) + '}', t)
    cf_timestamps.append(m2.group(0) if m2 else "")
  filter_lst = [t > timestamp for t in cf_timestamps]
  return [change_files[i] for i in range(len(change_files)) if filter_lst[i]]

def filter_compustat_files(compustat_files):
  timestamps = []
  for t in compustat_files:
    m = re.search('[0-9]{8,}', t)
    if m:
      timestamps.append(m.group(0))
  if not timestamps:
    return []
  timestamps.sort()
  last_timestamp = timestamps[-1]
  return [f for f in compustat_files if last_timestamp in f]

def download(file_dic):
  try:
    host = config('FTP_HOST')
    username = config('XF_USERNAME')
    password = config('XF_PASSWORD')

    # ftp connection
    sftp, transport = connect(host, username, password)
    top_dir, package, file_name = file_dic

    sftp.chdir(top_dir)
    sftp.chdir(package)

    f = os.path.join(top_dir, package, file_name)

    size = sftp.stat(file_name).st_size
    if os.path.isfile(f) and size == os.path.getsize(f):
      print(f + " is already downloaded.")
      sftp.close()
      transport.close()
      return
    with open(f, 'wb') as ff, tqdm(unit='B', unit_scale=True, leave=True, miniters=1, desc="Downloading " + file_name + "...", total=size) as tq:
      def _callback(transferred, total):
        tq.n = transferred
        tq.refresh()
      sftp.getfo(file_name, ff, callback=_callback)
    sftp.close()
    transport.close()
  except Exception as e:
    print('An exception occurred : {}'.format(e))

if __name__ == "__main__":
  BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
  try:
    host = config('FTP_HOST')
    username = config('XF_USERNAME')
    password = config('XF_PASSWORD')
    destination = config('POSTBOX_DESTINATION')
  except Exception as e:
    print("환경변수 설정을 확인하세요:", e)
    exit(1)

  # local dir change and setup top most directories
  if not os.path.exists(destination):
    os.makedirs(destination)

  os.chdir(destination)
  for dir in ['Products', 'Inbox', 'Outbox', 'Xpressfeed']:
    if not os.path.exists(dir):
      os.mkdir(dir)

  # ftp connection
  sftp, transport = connect(host, username, password)

  # Move to products first
  sftp.chdir('Products')
  print('Move to Products')
  packages = sftp.listdir()
  top_dir = 'Products'

  download_files = []
  for package in packages:
    # package filter
    if package not in [
      'CompanyRelReference'
    ]:
      continue

    sftp.chdir(package)
    print('Move to ' + os.path.join(top_dir, package))
    p = os.path.join(top_dir, package)
    if not os.path.exists(p):
      os.makedirs(p)

    files = sftp.listdir()

    # download feed config
    if package == 'XpressfeedFeedConfigV2':
      files.sort()
      if files:
        download_files.append((top_dir, package, files[-1]))
      sftp.chdir('..')
      continue

    # download installation files
    if package in ['V5Loader_Linux', 'V5Loader_Windows']:
      download_files.extend([(top_dir, package, f) for f in files])
      sftp.chdir('..')
      continue

    full_flags = [f for f in files if "Full" in f and f.endswith("flg")]
    if full_flags:
      full_flags.sort()
      last_full_flag = full_flags[-1]
      download_files.append((top_dir, package, last_full_flag))
    else:
      print("There is no full flags in " + package)

    full_files = [f for f in files if "Full" in f and f.endswith("zip")]
    valid_fulls = filter_full_files(full_files)
    download_files.extend([(top_dir, package, vf) for vf in valid_fulls])

    change_files = [
      f for f in files if "Change" in f and f.endswith("zip")]
    if change_files and valid_fulls:
      valid_changes = filter_change_files(valid_fulls[-1], change_files)
      download_files.extend([(top_dir, package, vc) for vc in valid_changes])
    else:
      print("There is no change files in " + package)
    sftp.chdir('..')

  sftp.chdir('..')
  if 'Xpressfeed' in sftp.listdir('.'):
    sftp.chdir('Xpressfeed')
    print('Move to Xpressfeed')
    packages = sftp.listdir()
    top_dir = 'Xpressfeed'
    for package in packages:
      break

      sftp.chdir(package)
      print('Move to ' + os.path.join(top_dir, package))
      p = os.path.join(top_dir, package)
      if not os.path.exists(p):
        os.makedirs(p)

      files = sftp.listdir()

      # package filtering
      # if package not in ['aBANK01']:
      #   sftp.chdir('..')
      #   continue

      if package in ['suppcxf']:
        download_files.extend([(top_dir, package, lf) for lf in files])
        sftp.chdir('..')
        continue

      full_flags = [f for f in files if f.startswith("f_") and f.endswith("flg")]
      if full_flags:
        full_flags.sort()
        last_full_flag = full_flags[-1]
        download_files.append((top_dir, package, last_full_flag))
      else:
        print("There is no full flags in " + package)

      full_files = [f for f in files if f.startswith("f_") and f.endswith("zip")]
      valid_fulls = filter_full_files(full_files)
      download_files.extend([(top_dir, package, vf) for vf in valid_fulls])

      change_files = [
        f for f in files if f.startswith("t_")]
      if change_files and valid_fulls:
        valid_changes = filter_change_files(valid_fulls[-1], change_files)
        download_files.extend([(top_dir, package, vc) for vc in valid_changes])
      else:
        print("There is no change files in " + package)
      sftp.chdir('..')
  sftp.close()
  transport.close()

  print("File scanning done.")
  thread_count = max(1, os.cpu_count() - 1 if os.cpu_count() else 4)
  thread_pool = ThreadPool(thread_count)
  for _ in tqdm(thread_pool.imap_unordered(download, download_files), total=len(download_files), desc="Downloading files"):
    pass
  thread_pool.close()
  thread_pool.join()
  print("File download done.")