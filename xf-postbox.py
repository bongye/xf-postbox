from ftplib import FTP_TLS
import os
import re
from tqdm import tqdm
from decouple import config
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool


def connect(host, username, password):
  ftps = FTP_TLS()
  #ftps.debugging = 2
  ftps.connect(host=host, port=21)
  ftps.login(username, password)
  ftps.prot_p()
  return ftps


def filter_full_files(full_files):
  full_files.sort()
  last_full_file = full_files[-1]
  m = re.search('[0-9]{8,}', last_full_file)
  timestamp = m.group(0)
  return list(filter(lambda t: timestamp in t, full_files))


def filter_change_files(last_full_file, change_files):
  m = re.search('[0-9]{8,}', last_full_file)
  timestamp = m.group(0)
  cf_timestamps = [
      re.search('[0-9]{' + str(len(timestamp)) + '}', t).group(0) for t in change_files]
  filter_lst = [t > timestamp for t in cf_timestamps]
  return [change_files[i] for i in range(len(change_files)) if filter_lst[i]]


def filter_compustat_files(compustat_files):
  timestamps = [re.search('[0-9]{8,}', t).group(0) for t in compustat_files]
  timestamps.sort()
  last_timestamp = timestamps[-1]
  return [f for f in compustat_files if last_timestamp in f]


def download(file_dic):
  try:
    host = config('EDX_HOST')
    username = config('XF_USERNAME')
    password = config('XF_PASSWORD')

    # ftp connection
    ftps = connect(host, username, password)
    top_dir, package, file_name = file_dic

    ftps.cwd(top_dir)
    ftps.cwd(package)

    f = os.path.join(top_dir, package, file_name)

    size = ftps.size(file_name)
    if os.path.isfile(f) and size == os.path.getsize(f):
      print(f + " is already downloaded.")
      return
    with open(f, 'wb') as ff, tqdm(unit='blocks', unit_scale=True, leave=True, miniters=1, desc="Downloading " + file_name + "...", total=size) as tq:
      def _callback(chunk):
        ff.write(chunk)
        tq.update(len(chunk))
      ftps.retrbinary('RETR %s' % file_name, _callback)
      ff.close()
  except Exception as e:
    print('An exception occurred : {}'.format(e))


if __name__ == "__main__":
  BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
  host = config('EDX_HOST')
  username = config('XF_USERNAME')
  password = config('XF_PASSWORD')

  # local dir change and setup top most directories
  destination = config('POSTBOX_DESTINATION')
  if not os.path.exists(destination):
    os.makedirs(destination)

  os.chdir(destination)
  for dir in ['Products', 'Inbox', 'Outbox', 'Xpressfeed']:
    if not os.path.exists(dir):
      os.mkdir(dir)

  # ftp connection
  ftps = connect(host, username, password)

  # Move to products first
  ftps.cwd('Products')
  print('Move to Products')
  packages = ftps.nlst()
  top_dir = 'Products'

  download_files = []
  for package in packages:
    # package filter
    # if package not in ['XpressfeedFeedConfigV2', 'V5Loader_Linux', 'V5Loader_Windows']:
    #  continue

    ftps.cwd(package)
    print('Move to ' + os.path.join(top_dir, package))
    p = os.path.join(top_dir, package)
    if not os.path.exists(p):
      os.makedirs(p)

    files = ftps.nlst()

    # download feed config
    if package == 'XpressfeedFeedConfigV2':
      files.sort()
      download_files.append((top_dir, package, files[-1]))
      ftps.cwd('..')
      continue

    # download installation files
    if package in ['V5Loader_Linux', 'V5Loader_Windows']:
      download_files.extend([(top_dir, package, f) for f in files])
      ftps.cwd('..')
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
    if change_files:
      valid_changes = filter_change_files(valid_fulls[-1], change_files)
      download_files.extend([(top_dir, package, vc) for vc in valid_changes])
    else:
      print("There is no change files in " + package)
    ftps.cwd('..')

  ftps.cwd('../Xpressfeed')
  print('Move to Xpressfeed')
  packages = ftps.nlst()
  top_dir = 'Xpressfeed'
  for package in packages:
    ftps.cwd(package)
    print('Move to ' + os.path.join(top_dir, package))
    p = os.path.join(top_dir, package)
    if not os.path.exists(p):
      os.makedirs(p)

    files = ftps.nlst()

    # package filtering
    #if package not in ['aBANK01']:
    #  ftps.cwd('..')
    #  continue

    if package in ['suppcxf']:
      download_files.extend([(top_dir, package, lf) for lf in files])
      ftps.cwd('..')
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
    if change_files:
      valid_changes = filter_change_files(valid_fulls[-1], change_files)
      download_files.extend([(top_dir, package, vc) for vc in valid_changes])
    else:
      print("There is no change files in " + package)
    ftps.cwd('..')

  print("File scanning done.")
  thread_count = cpu_count() - 1
  thread_pool = ThreadPool(thread_count)
  thread_pool.imap(download, download_files)
  thread_pool.close()
  thread_pool.join()
  print("File download done.")
