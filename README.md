check_barman_restore
===================

Run test DB recovery of Barman backups

SYNOPSIS
--------

    check_barman_restore.py [-c <"cluster_list">] [-r <restore_host>] [-L <log_level>] [-l <log_file>] [-m <"email_list">] [-w <wait_for_recovery_min>] [-k]

DESCRIPTION
-----------

`check_barman_restore.py` is intended to run regular test backup recovery of barman backups.

If you don't test your backups, they may not work when you really need them. That script
scheduled in cron, constantly verifies the backups. It will restore a backup for each database,
verify that db responds for a simple query, and drop the restored db before going to the next one.

It is based on an original art by Vladimir Borodin:
https://github.com/dev1ant/misc/tree/master/backups_checking/check_backup_consistency.py

 The script was significantly refactored:
 - It is possible to run a test recovery on a remote or local host
 - A recovered database has postgres user ownership 
 - Use the barman's native recover command, so it is closely mimics a real production recovery
 - Target db clusters may be passed as parameters, as opposed to checking all the clusters in the barman configuration
 - Logging configuration flexibility is added
 - Email notification is possible in case of a recovery problem
 - It is possible to keep the recovered database for further investigation, instead of dropping it

PARAMETERS
----------

      -h
        Usage help

      -c, --cluster_list
        A List of database clusters to restore ( servers in the Barman's terminology).  These are names from server config files in /etc/barman.d directory
        These clusters are identified by a line in the configuration file, in square brackets ([ and ]).
        The server section represents the ID of that server in Barman.
        The list should be coma separated without spaces, or enclosed in quotations.
        Examples:
                -c 'testclustr1 testcluster2'    or
                -c testclustr1,testcluster2

      -r, --restore_host
        The name of the host where the test restores will be executed. Default is `localhost`, but any host available over ssh may be used.
        The ssh connection is established from barman user on barman host to postgres user on the restore host.
        Even if the default localhost is used, you still need make sure that barman can connect like this:
                barman$ ssh postgres@localhost

      -L, --log_level
        Supported levels: ERROR, WARNING, INFO, DEBUG

      -l, --log_file
        Full log file path. If not specified, the output will go to STDOUT

      -m, --email_list
        A comma separated ( no spaces ) list of e-mails. If –m is specified, an e-mail will be sent in case of a problem

      -w, --wait_for_recovery_min
        How long in minutes to wait for a db to recover the wal before declare failure. The default is 180 minutes.
        The allowed range is from 1 to 180 minutes.

      -k, --keep
        Do not remove the recovered database after a recovery. That mode may be used for a db problems investigation.
        However, if -k mode was used, the consecutive check_barman_restore.py executions for the same pgdata path will refuse to overwrite the files and fail.
        A manual cleanup of recovery pgdata location will be necessary on the recover host.

- - -

The script was tested with Python 2.7.5, Postgresql 9.6 and  9.5

CONTRIBUTORS
------------

* [Vladimir Borodin] (https://github.com/dev1ant)
* [Igor Polishchuk] (https://github.com/ipolishchuk)
