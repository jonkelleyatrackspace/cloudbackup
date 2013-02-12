## Environment setup
pip-2.6 install aaargh
yum install gcc make python26-devel
pip-2.6 install beefish pycrypto boto python-cloudfiles 

cd /opt
git clone git://github.com/jonkelleyatrackspace/cloudbackup.git
cd cloudbackup

# Make a configuration file
# Using filewalker,
#  Filewalker will recursively walk through directories and push them to cloudfiles. Very convienent.
#  You can also make it delete files after uploading.
#  Files that error during upload will not be deleted.

# Make a config that looks like: filewalker.conf
[filewalker]
# How old to be in seconds before backing up.
backup_age = 34560000

# Delete after backup?
delete_afterwards = True

# Source of files to back up. REMEBER TRAILING SLASH.
backup_source = /mnt/log/
backup_password = <ENCRYPTION PASS>

[cloudfilesSettings]
apiuser = yyys
apikey = xxx
container = CONTAINER_TO_BACK_TO
region_name = ord


# Execute a backup WITHOUT PERFORMING ANY OPERATION, use this first!
python2.7 filewalker.py backup --config filewalker.conf --noop true

# Execute a backup based on your settings:
python2.7 filewalker.py backup --config filewalker.conf

# Restore a backup from remote end.
# Remember to add .enc if it is an encrypted file!
# You will be asked for the crypto password, and the file will be extracted in the local directory.
python2.7 filewalker.py restore -f name-of-file.bz2.tgz.enc --config filewalker.conf

