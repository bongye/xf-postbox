from ftplib import FTP_TLS
import os
import re
from tqdm import tqdm
from decouple import config


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
  m = re.search('[0-9]+', last_full_file)
  timestamp = m.group(0)
  return list(filter(lambda t: timestamp in t, full_files))


def filter_change_files(last_full_file, change_files):
  m = re.search('[0-9]+', last_full_file)
  timestamp = m.group(0)
  cf_timestamps = [
      re.search('[0-9]{' + str(len(timestamp)) + '}', t).group(0) for t in change_files]
  filter_lst = [t > timestamp for t in cf_timestamps]
  return [change_files[i] for i in range(len(change_files)) if filter_lst[i]]


def download(ftps, file_name):
  size = ftps.size(file_name)
  with open(file_name, 'wb') as f, tqdm(unit='blocks', unit_scale=True, leave=True, miniters=1, desc="Downloading " + file_name + "...", total=size) as tq:
    def _callback(chunk):
      f.write(chunk)
      tq.update(len(chunk))
    ftps.retrbinary('RETR %s' % file_name, _callback)
    f.close()


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
  os.chdir('Products')
  packages = ftps.nlst()

  for package in packages:
    if package != 'GVKeyEnhanced':
      continue

    ftps.cwd(package)
    if not os.path.exists(package):
      os.mkdir(package)
    os.chdir(package)

    files = ftps.nlst()
    full_flags = [f for f in files if "Full" in f and f.endswith("flg")]
    if full_flags:
      full_flags.sort()
      last_full_flag = full_flags[-1]
      download(ftps, last_full_flag)
    else:
      print("There is no full flags in " + package)

    full_files = [f for f in files if "Full" in f and f.endswith("zip")]
    valid_fulls = filter_full_files(full_files)
    for vf in valid_fulls:
      download(ftps, vf)
    last_full_file = full_files[-1]
    change_files = [f for f in files if "Change" in f and f.endswith("zip")]
    if change_files:
      valid_changes = filter_change_files(last_full_file, change_files)
      for vc in valid_changes:
        download(ftps, vc)
    else:
      print("There is no change files in " + package)
    ftps.cwd('..')
    break
  ftps.quit()
