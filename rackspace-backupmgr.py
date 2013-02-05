#!/usr/bin/python
#
"""
    MadeBy:         Jon Kelley
    Published:      Dec 2, 2012
    Synopsis:       Wrapper that handles backup retention policies for pushing files to cloudfiles.
    Requires:       Python 2.6
    Requires Libs:  pip install python-cloudfiles boto beefish pycrypto aaargh
    Description:    This program loops through a target backup directory (backup_location) 
                    and looks for files matching an expression.
                    
                    If files match, they are then uploaded to the cloud either using compression
                    or compression+encryption using blowfish (supported by the beefish+pycrypto libraries)

                    Post-upload to the cloud, a post_backup_action is performed. The first action
                    justdelete, will trash a file immediately , the second option purgatory
                    will keep the file in a staging area on-disk for a temporary time,
                    until deleting the file from the staging area after a user-defined period of time.
                    
                    If you are new and just want to get started, type:
                        python rackspace-backupmgr.py makeconfig
                        
                    Once you have generated your config, you can start a backup with:
                        python rackspace-backupmgr.py backup -c <path to the config created above>
"""

import signal, sys
if sys.version_info < (2,6):
    raise SystemExit('Sorry, needs Python 2.6 or higher.')
if sys.version_info > (3,0):
    raise SystemExit('Sorry, does not support the great syntax change of Python 3.')

import os, glob, time, os.path
import pycloudbackup    # For backup_file()
import aaargh           # For parsing args everywhere.
import ConfigParser     # For parsing configs everywhere.
import logging          # Guess.

def signal_handler(signal, frame):
    #print "\n\nYou killed it!"
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


log = logging.getLogger(__name__)

if not log.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

config = ConfigParser.SafeConfigParser()
app = aaargh.App(description="Handles backups, including local backup retention.")

@app.cmd(help="Starts backup/archiving process.")
@app.cmd_arg('-c', '--configfile', type=str)
def backup(configfile):
    if not configfile:
        log.error("Configuration not found. Use --config arg to define config file.")
        return
    else:
        config.read(os.path.expanduser(configfile))
    backup_isenabled = config.get("backupSettings", "backup_isenabled")
    backup_location  = config.get("backupSettings", "backup_location")
    matches          = config.get("backupSettings", "backup_files_matching")
    post_backup_action = config.get("backupSettings", "post_backup_action")
    purge_isenabled  = config.get("backupSettings", "purge_isenabled")

    
    if backup_isenabled == "True" and is_directory(backup_location):
        log.debug("DEBUG: Considering objects that fit this criterion for backup: " + str(backup_location) + str(matches) )
        for file in glob.glob(os.path.join(backup_location, matches)): # Iterates through backup dir
            backup_file(file)
            if post_backup_action == "purgatory":
                if purge_isenabled == "True":
                    purge_aftersecs  = config.get("backupSettings", "purge_after_secs")
                    purge_location   = config.get("backupSettings", "purgatory_location")
                    purge_file(file,purge_location)
                else:
                    log.info("Purge disabled or purge directory not found.")
            elif post_backup_action == "justdelete":
                delete_file(file)
            else:
                log.warn("No post-backup actions [purgatory,justdelete] found. I dunno what to do!!!!!!!!!!!")
    else:
        log.warn("Backups are disabled or backup directory not existant.")

    # Delete old files in purgatory
    if purge_isenabled == "True":
        purge_deletePurgedItems(purge_location,purge_aftersecs)

@app.cmd(help="Lists currently configured container.")
@app.cmd_arg('-c', '--configfile', type=str) # doesnt work quite right yet
def ls(configfile):
    if not configfile:
        log.error("Configuration not found. Use --config arg to define config file.")
        return
    else:
        config.read(os.path.expanduser(configfile))
    apiuser     = config.get("cloudfilesSettings", "apiuser")
    apikey      = config.get("cloudfilesSettings", "apikey")
    container   = config.get("cloudfilesSettings", "container")
    region_name = config.get("cloudfilesSettings", "region_name")
    crypto_password = config.get("backupSettings", "crypto_password")
    backup_constants = {"apiuser": apiuser,
                        "apikey": apikey,
                        "container": container,
                        "region_name": region_name,
                        "crypto_password": crypto_password }

    pycloudbackup.ls(destination="cloudfiles")

@app.cmd(help="Starts restore process.")
@app.cmd_arg('-c', '--configfile', type=str)
@app.cmd_arg('-f', '--filename', type=str)
def restore(configfile,filename=None,cryptopass=None):
    if not configfile:
        log.error("Configuration not found. Use --config arg to define config file.")
        return
    else:
        config.read(os.path.expanduser(configfile))
        
    if not filename:
        log.error("Requires a filename to restore on the remote end.")

    backup_isenabled = config.get("backupSettings", "backup_isenabled")
    backup_location  = config.get("backupSettings", "backup_location")
    matches          = config.get("backupSettings", "backup_files_matching")
    post_backup_action = config.get("backupSettings", "post_backup_action")
    purge_isenabled  = config.get("backupSettings", "purge_isenabled")

    restore_file(filename,cryptopass)
    print filename + " restored to CWD."

def restore_file(file,crytopass):
    apiuser     = config.get("cloudfilesSettings", "apiuser")
    apikey      = config.get("cloudfilesSettings", "apikey")
    container   = config.get("cloudfilesSettings", "container")
    region_name = config.get("cloudfilesSettings", "region_name")
    crypto_password = config.get("backupSettings", "crypto_password")
    backup_constants = {"apiuser": apiuser,
                        "apikey": apikey,
                        "container": container,
                        "region_name": region_name,
                        "crypto_password": crypto_password }

    pycloudbackup.restore(file, conf=backup_constants, destination="cloudfiles")

def file_older_than(file,required_delta):
    """ Returns true or false if file is older than specified required_delta
        Uses cmtime to get last modified date, its important to know the distinction because I made a mistake.
            * ctime - last time the properties of file modified
                ie: ownership, location, permissions, stuff the filesystem cares about
            * mtime - last time the CONTENTS of file modified
        Returns TRUE if is older, else FALSE if too young.
    """
    epoch_mtime  = os.path.getctime(file)
    epoch_now   = time.time()
    epoch_delta = epoch_now - epoch_mtime

    if epoch_delta >= required_delta:
        return True
    else:
        return False

def purge_file(file,purge_location):
    """ Sends file to purgatory, file is a absolute path, so will need to strip it """
    file_basename = os.path.basename(file)
    log.info("Purgatory is aquiring " + file_basename)
    if is_directory(purge_location):
        os.rename(file,purge_location + file_basename)
    else:
        log.warn("Purge directory not found")

def purge_deletePurgedItems(purge_location,purge_aftersecs):
    purge_aftersecs = int(purge_aftersecs)
    if is_directory(purge_location): # If purge directory exists
        purge_dir = os.listdir(purge_location) # List items in purge directory
        for file in purge_dir: # For file name in list
            file = purge_location + file  # Build absolute path from relative file path

            if file_older_than(file,purge_aftersecs): # If file greater than defined epoch
                delete_file(file) # Delete
    else:
        log.warn("Purge directory not found")
 
def delete_file(file):
    """ Deletes a file, accepts 1 arguement: the file you wish to destroy """
    log.info("Deleting file " + file)
    os.unlink(file) # cya.

def backup_file(file):
    apiuser     = config.get("cloudfilesSettings", "apiuser")
    apikey      = config.get("cloudfilesSettings", "apikey")
    container   = config.get("cloudfilesSettings", "container")
    region_name = config.get("cloudfilesSettings", "region_name")
    crypto_password = config.get("backupSettings", "crypto_password")
    """ Backups a file, accepts 1 arguement: the file you wish to backup """

    backup_constants = {"apiuser": apiuser,
                        "apikey": apikey,
                        "container": container,
                        "region_name": region_name,
                        "crypto_password": crypto_password }

    pycloudbackup.backup(file, conf=backup_constants, destination="cloudfiles")

def is_directory(dir):
    return os.path.isdir(dir)
    
@app.cmd(help="Configures backup manager.")
def makeconfig():
    print ("_______________________________________")
    print ("BACKUP MGR automatic configurator thing")

    config.add_section("cloudfilesSettings")
    print("=====================")
    print("CLOUDFILES PROPERTIES")
    print("   We got to store our backups somewhere, so we got to know")
    print("   where and with what credentials etc. Tell me.")
    config.set("cloudfilesSettings", "apiuser",     raw_input("Cloudfiles User      : "           ))
    config.set("cloudfilesSettings", "apikey",      raw_input("Cloudfiles Key       : "           ))
    config.set("cloudfilesSettings", "container",   raw_input("Cloudfiles Container : "           ))
    config.set("cloudfilesSettings", "region_name", raw_input("Cloudfiles Region [ord,lon] : "))

    config.add_section("backupSettings")
    print("=======================")
    print("LOCAL BACKUP PROPERTIES")
    config.set("backupSettings", "backup_isenabled", "True")
    
    print("----------------")
    print("BACKUP DIRECTORY")
    print("   Which directory do your backups reside in? IE: /var/backup/")
    print("   CAPT OBVIOUS: Full path on disk. Always a trailing slash.")
    backup_path=raw_input("Backup Directory: ")
    if backup_path.endswith('/'):
        config.set("backupSettings", "backup_location", backup_path)
    else:
        print("==================WARNING!: You didn't put a trailing slash on directory, assuming you meant to put: " + str(backup_path) + "/ ..." )
        time.sleep(2)
        config.set("backupSettings", "backup_location", str(backup_path) + "/")
    
    
    print("--------------------")
    print("BACKUP FILE MATCHING")
    print("   We can match particular filenames in part or whole within the aforementioned backup directory.")
    print("   What filenames should we match? You can use wildcards such as *, ?, and [ ] style ranges.")
    print("   IE: *.sql.tar.?? would match backup396393.sql.tar.gz")
    print("   IE: * would match anything.")
    ans=raw_input("Backup files matching: ")
    config.set("backupSettings", "backup_files_matching", ans)

    print("===================")
    print("POST-BACKUP ACTIONS")
    print("   What happens after a backup is put on cloudfiles? Options include:")
    print("   * justdelete - Just delete the file from our backup folder.")
    print("   * purgatory  - Save the file in a safe local disk place for a limited time.")
    ans=raw_input("What should we do here?: ")
    config.set("backupSettings", "post_backup_action", ans)
    if ans == "purgatory":
        config.set("backupSettings", "purge_isenabled", "True")
        print("------------------")
        print("PURGATORY DIRECTORY")
        print("   Once files are backed-up, what directory should we temporarily store them in?")
        print("   I.E. /var/purgatory or /var/backup/purgatory (as long as you don't back up files matching purgatory.)")
        print("   CAPT OBVIOUS: Full path on disk. Always a trailing slash.")
        ans = raw_input("What directory?:")
        if ans.endswith('/'):
            config.set("backupSettings", "purgatory_location", ans)
        else:
            print("================WARNING!: You didn't put a trailing slash on directory, assuming you meant to put: " + str(ans) + "/ ..." )
            time.sleep(2)
            config.set("backupSettings", "purgatory_location", str(ans) + "/")
        
        print("------------------------------")
        print("PURGE PURGATORY AFTER... WHEN?")
        print("   After a set interval you should purge the purgatory so your disk doesn't fill up.")
        print("   How long in seconds? 172800 is 2 days. That's a nice one.")
        config.set("backupSettings", "purge_after_secs", raw_input("How long: "))
    else:
        config.set("backupSettings", "purge_isenabled", "False")

    print("=====================")
    print("CRYPTOGRAPHY SETTINGS")
    print("   Should we encrypt the file on Cloud Files using blowfish?")
    print("   If you just want it stored plain-text compressed")
    print("     you can answer this with None.")
    config.set("backupSettings", "crypto_password", raw_input("Crypto Password: ") )

    print("================")
    print("SAVE CONFIG FILE")
    print("   The config you are about to generate will be stored in the current working directory.")
    print("   What do you want to call it?")
    configname = raw_input("Config Filename [backupmgr.conf]: ") 
    if not configname:
        configname = "backupmgr.conf"
    config.write(open(os.path.expanduser("./" + configname), "w"))
    log.info("Config written in %s" % os.path.expanduser("./" + configname))

def main():
    app.run()

if __name__ == '__main__':
    main()


#import os, time
#dirList = os.listdir("./")
#for d in dirList:
#    if os.path.isdir(d) == True:
#        stat = os.stat(d)
#        created = os.stat(d).st_mtime
#        asciiTime = time.asctime( time.gmtime( created ) )
#        print d, "is a dir  (created", asciiTime, ")"
#    else:
#        stat = os.stat(d)
#        created = os.stat(d).st_mtime
#        asciiTime = time.asctime( time.gmtime( created ) )
#        print d, "is a file (created", asciiTime, ")"
