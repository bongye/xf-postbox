# xf-postbox
A simple python script to extract raw zip file from a client edx ftp account.
Since the download speed of sftp, use ftp over tls host instead.

This script is not to download whole files from ftp account.
Based on full file timestamp on its file name, this script filtering last full file(s) and changed files after the last full file.

**Prerequisite**
```
$ pip install python-decouple

# set host, username, password and destination folder on .env file
$ vim .env