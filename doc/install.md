# Installation on Debian 9

## 1. Install Python 3.6 or newer

## 2. Clone repository
1. Switch to home directory `cd ~`
2. `git clone https://git.informatik.uni-rostock.de/iuk/security-projects/software/building-automation/log-ip-to-db.git`

## 3. Install dependencies
1. `pip3 install --user git+https://git.informatik.uni-rostock.de/iuk/security-projects/software/building-automation/knx-parser.git`
2. `pip3 install --user mysql-connector`

## 4. Add to autostart
1. Edit crontab with `crontab -e`
2. Add these lines (it might be necessary to edit the script path):

```sh
PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/bin:/usr/local/sbin

@reboot cd /home/knxlog/log-ip-to-db/src/ && ./run.sh
```

3. Make sure crontab service is enabled and running: `systemctl status cron.service`
