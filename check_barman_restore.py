#!/usr/bin/python

import subprocess
import time
import json
import os
import sys
import re
import psycopg2
import logging
import datetime
import getopt
import getpass
import socket
import smtplib
from email.mime.text import MIMEText
from barman import lockfile

cluster_list=[]
restore_host='localhost'
log_level='INFO'
log_file=''
email_list=[]
# How long ( in minutes) wait for db recovery
wait_for_recovery_min=180  # wait for 3 hours
keep=False

def main(argv):
   global cluster_list
   global restore_host
   global log_level
   global log_file
   global email_list
   global wait_for_recovery_min
   global keep

   try:
      opts, args = getopt.getopt(argv,'hc:r:L:l:m:w:k',['cluster_list=','restore_host=','log_level=','log_file=','email_list=','wait_for_recovery_min=','keep='])
   except getopt.GetoptError as err:
      print str(err)
      usage(2)

   if args:
      logging.error(' Extra values are left in the command line after parsing the arguments: %s ' % ' '.join(args))
      logging.error(' Parameters --cluster_list and --restore_host values should be coma separated or enclosed in quotations')
      usage(2)

   for opt, arg in opts:
      if opt == '-h':
         usage(0)
      elif opt in ('-c', '--cluster_list'):
         cluster_list = re.split('\W+',arg)
      elif opt in ('-r', '--restore_host'):
         restore_host = arg
      elif opt in ('-L', '--log_level'):
         log_level = arg
      elif opt in ('-l', '--log_file'):
         log_file = arg
      elif opt in ('-m', '--email_list'):
         email_list = re.split('[^a-zA-Z0-9_@.-]',arg)
      elif opt in ('-w', '--wait_for_recovery_min'):
         wait_for_recovery_min = int(arg)
      elif opt in ('-k', '--keep'):
         keep = True

def verify_params(bd):
   invalid_clusters = list(set(cluster_list)-set(get_list_of_clusters(bd)))
   if invalid_clusters:
      print ' Parameter -c (--cluster_list) has invalid values %s' % ','.join(invalid_clusters)
      usage(1)
   
   for cluster in cluster_list:
      source_db_host = get_source_db_host(bd,cluster)
      if restore_host == source_db_host:
         print ' Parameter --restore_host=%s matches a source database host name, which is not alowed in %s for safety reasons.' % (source_db_host, sys.argv[0])
         print ' Use "barman recover" command instead'
         usage(1)
   
   if log_level not in ['ERROR','WARNING','INFO','DEBUG']:
     print ' Parameter --log_level is %s. It  should be one of ERROR,WARNING,INFO,DEBUG' % log_level
     usage(1)
  
   # Verify that we can create log file in the requested location
   if log_file:
      try:
         fhandle = open(log_file, 'a')
         os.utime(log_file, None)
      except IOError as err:  
         print " Problem with log file: %s" % str(err)
         usage(1)
      else:
         if fhandle: fhandle.close() 

   for address in email_list:
      if not re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', address):
         print 'email address %s is mulformed' % address
         usage(1) 
         
   if (wait_for_recovery_min < 1 or wait_for_recovery_min > 180):
      print 'allowed range for wait_for_recovery_min is 1 .. 180 minutes. You have %d' % wait_for_recovery_min
      usage(1)

def print_params():
   logging.info(" Parameters:")
   logging.info(" cluster_list: %s " % cluster_list)
   logging.info(" restore_host: %s " % restore_host)
   logging.info(" log_level: %s " % log_level)
   logging.info(" log_file: %s " % log_file)
   logging.info(" email_list: %s " % email_list)
   logging.info(" wait_for_recovery_min: %s " % wait_for_recovery_min)
   logging.info(" keep: %s " % str(keep))

def usage(code):
  print 'Usage: %s [-c <"cluster_list">] [-r <restore_host>] [-L <log_level>] [-l <log_file>] [-m <"email_list">] [-w <wait_for_recovery_min>]' % sys.argv[0]
  sys.exit(code)

def send_mail(cluster):
   from_address = '%s@%s' %  (getpass.getuser(),socket.gethostname())
   lines = ''
   if log_file: 
      read_n_last_bytes = 4096
      fp = open(log_file, 'r')
      if os.stat(log_file).st_size > read_n_last_bytes:
        fp.seek(-read_n_last_bytes,2)
        lines = 'The last %d bytes of the log file %s:\n'  % (read_n_last_bytes,log_file)
      for line in fp:
        lines += line
      msg = MIMEText(lines)
      fp.close()
   else:
      msg = MIMEText('Log file was not specified')
   
   msg['Subject'] = 'Errors in %s with cluster %s' % (sys.argv[0],cluster)
   msg['From'] = from_address
   msg['To'] = ','.join(email_list)
   
   logging.debug(' Sending email about a problem with %s to %s' % (cluster,','.join(email_list)))
   try:
      s = smtplib.SMTP('localhost')
      s.sendmail(from_address, email_list, msg.as_string())
      s.quit() 
   except Exception as err:
      logging.error(' Problem with sending email to %s: %s' % (','.join(email_list),str(err)))

def get_diagnose():
   cmd = 'barman diagnose'
   answer = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE).communicate()[0]
   return json.loads(answer)

def get_list_of_clusters(bd):
   return bd['servers'].keys()

def get_last_backup(bd,cluster):
   backups_info = bd['servers'][cluster]['backups']
   # Get the list of all the done backups
   done_backups = [ b for b in backups_info.keys() if backups_info[b]['status'] == 'DONE' ]
   # Return the last backup if not empty
   if done_backups:
      return max(done_backups)
   else:
      return None

def execute_command(cmd):
   proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
   (out, err) = proc.communicate()
   if out:
      logging.info(' Command stdout:')
      logging.info(out)
   if err:
      logging.error(' Command stderr:')
      logging.error(err)
   return proc.returncode

def restore_backup(cluster,backup,pgdata):
   cmd = '/bin/barman recover --remote-ssh-command="ssh postgres@%s" --get-wal --target-immediate %s %s %s' % (restore_host,cluster,backup,pgdata)
   logging.debug(' Restore command: %s' % cmd)
   res = execute_command(cmd)
   return res

def hack_configs(path):
   cmd = '''ssh postgres@%s sed -i %s/postgresql.conf -e "/^shared_preload_libraries/s/'.*'/''/" ''' % (restore_host,path)
   res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
   if res != 0:
      return res

   cmd = '''ssh postgres@%s sed -i %s/postgresql.conf -e "/^shared_buffers/s/=\ .*$/=\ 8GB/" ''' % (restore_host,path)
   res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
   if res != 0:
      return res

   cmd = '''ssh postgres@%s sed -i %s/postgresql.conf -e "/^stats_temp_directory/s/\ =\ .*$/\ =\ \'pg_stat_tmp\'/" ''' % (restore_host,path)
   res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
   if res != 0:
      return res

   escaped = path.replace('/', '\/') + '\\/pg_hba.conf'
   cmd = '''ssh postgres@%s sed -i %s/postgresql.conf -e "/^hba_file/s/'.*'/'%s'/" ''' % (restore_host,path, escaped)
   res = subprocess.call(cmd, shell=True, stderr=sys.stderr, stdout=sys.stdout)
   if res != 0:
      return res
   
   return 0

def start_postgres(bd,cluster,backup, path):
   version = get_pg_version(bd,cluster,backup)
   cmd = ' ssh postgres@%s /usr/pgsql-%s/bin/pg_ctl start -D %s < /dev/null >& /dev/null &' % (restore_host,version,path)
   logging.debug(cmd)
   res = execute_command(cmd)
   return res

def get_pg_version(bd,cluster,backup):
   full_version = bd['servers'][cluster]['backups'][backup]['version']
   # Before v10
   #version = '%d.%d' % (full_version/10000, full_version/100 % 100)
   version = '%s' % (full_version/10000)
   return version

def get_conn_string(bd,cluster):
   conninfo = bd['servers'][cluster]['config']['conninfo']
   host = get_source_db_host(bd,cluster)
   conninfo = conninfo.replace('host=%s' % host, 'host=%s' % restore_host)
   logging.debug(' conninfo=%s' % conninfo)
   return conninfo

def get_source_db_host(bd,cluster):
   conninfo = bd['servers'][cluster]['config']['conninfo']
   return re.search('(?<=host=)\w+', conninfo).group(0)

def check_consistency_of_one_backup(bd,cluster,backup):
   pgdata = bd['servers'][cluster]['backups'][backup]['pgdata']
   res = restore_backup(cluster,backup,pgdata)
   if res != 0:
      logging.error(' Cannot restore backup %s for cluster %s, the result code is %d' % (backup,cluster,res))
      return 1
   res = hack_configs(pgdata)
   if res != 0:
      logging.error(' Could not hack configs for %s. Skipping it.' % cluster )
      return 2
   res = start_postgres(bd,cluster,backup,pgdata)
   if res != 0:
      logging.error(' Could not start PostgreSQL for %s. Skipping it.' % cluster )
      return 3

   time.sleep(10)
   conninfo = get_conn_string(bd,cluster)
   for i in xrange(1, wait_for_recovery_min+1): 
      try:
         conn = psycopg2.connect(conninfo)
         cur = conn.cursor()
         cur.execute('SELECT 42;')
         if cur.fetchone()[0] == 42:
            logging.info(' Backup %s for %s is OK.' % (backup,cluster ))
            return 0
      except Exception as err:
         if 'could not connect to server: Connection refused' in str(err):
            logging.error(' DB server failed to start: %s' % err)
            return 4
         if 'the database system is starting up' not in str(err):
            logging.warning(err)
         time.sleep(60)
         logging.info(' Waited for %d minutes out of %d for postgres to reach consistent state on %s.' % (i,wait_for_recovery_min,restore_host) )
         continue
   logging.error(' PostgreSQL has not reached consistent state on %s after %d minutes.' % (restore_host,wait_for_recovery_min) )
   return 5

def recovery_location_exist(bd,cluster,backup):
   pgdata = bd['servers'][cluster]['backups'][backup]['pgdata']
   cmd = "ssh postgres@%s test -d %s" % (restore_host,pgdata)
   logging.debug(' Verify if recovery location already exist: %s' % cmd)
   res = execute_command(cmd)
   if res == 0:
      logging.error(' Recovery location %s:%s for cluster %s already exist. Skipping recovery to avoid accidental database corruption.' % (restore_host,pgdata,cluster))
      logging.info(' Verify if recovery location is correct and if drop_deployed_backup() was executed in a previous execution')
      return 6
   return 0

def drop_deployed_backup(bd,cluster,backup):
   pgdata = bd['servers'][cluster]['backups'][backup]['pgdata']
   version = get_pg_version(bd,cluster,backup)
   cmd = ' ssh postgres@%s /usr/pgsql-%s/bin/pg_ctl stop -m immediate -D %s' % (restore_host,version,pgdata)
   logging.debug(cmd)
   res = execute_command(cmd)
   if res != 0:
      return res
   time.sleep(5)
   cmd = ' ssh postgres@%s rm -rf %s' % (restore_host,pgdata)
   logging.debug(cmd)
   res = execute_command(cmd)
   return res

def init_logging(bd):
   level = getattr(logging, log_level)
   root = logging.getLogger()
   root.setLevel(level)
   _format = logging.Formatter('%(levelname)s\t%(asctime)s\t\t%(message)s')
   if log_file:
      _handler = logging.FileHandler(log_file)
   else:
      _handler = logging.StreamHandler()
   _handler.setFormatter(_format)
   _handler.setLevel(level)
   root.handlers = [_handler]


if __name__ == '__main__':
   main(sys.argv[1:])
   barman_data = get_diagnose()
   verify_params(barman_data)
   init_logging(barman_data)

   with lockfile.LockFile('/tmp/check_barman_restore.lock') as locked:
      if not locked:
         logging.warning(' Another process is checking backups already. Exiting.')
         sys.exit(0)

      print_params()
      status_file_path = '/tmp/check_barman_restore.status'
      if os.path.exists(status_file_path):
         status_file = open(status_file_path, 'r')
         ts, status, description = status_file.read().rstrip().split(';')
         last = datetime.datetime.fromtimestamp(float(ts))
         current_date = datetime.datetime.today()
         day_start = current_date.combine(current_date.date(), current_date.min.time())
         logging.debug(' last = %s; current_date = %s; day_start = %s' % (last,current_date,day_start))
         #if last > day_start:
         #   logging.info('Backups have already been checked today. Not doing anything.')
         #   sys.exit(0)
         status_file.close()

      problems = set()
		
      # If cluster_list is passed as a paramaeter, use it, otherwise get all the clusters
      if not cluster_list:
         cluster_list = get_list_of_clusters(barman_data)
      
      for cluster in cluster_list:
         backup = get_last_backup(barman_data,cluster)
         if backup: 
            if recovery_location_exist(barman_data,cluster, backup):
               problems.add(cluster)
            else:
               logging.debug(' cluster=%s; backup=%s' % (cluster,backup))
               if check_consistency_of_one_backup(barman_data,cluster,backup) != 0:
                  problems.add(cluster)
               if keep:
                  logging.info(' Parameter -k (keep) is specified. The restored backups won''t be deleted')
                  logging.info(' Manually delete recovery pgdata directory before the next run') 
               else:
                  if drop_deployed_backup(barman_data,cluster, backup) != 0:
                     problems.add(cluster)
         else:
            logging.error(' Cannot find the latest backup for cluster %s' % cluster)
            problems.add(cluster)
         if cluster in problems and email_list:
            send_mail(cluster)

      if len(problems) != 0:
         status = 1
         msg = ' Clusters with failed backups are %s. Take a look at them.' % ','.join(sorted(problems))
      else:
        status = 0
        msg = ' All backups are consistent.'

      logging.info(msg)
      status_file = open(status_file_path, 'w')
      status_file.write('%d;%d;%s\n' % (int(time.time()), status, msg))
      status_file.close()
      sys.exit(status) 
