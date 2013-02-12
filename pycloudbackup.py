#!/usr/bin/python
"""
Copyright (c) 2012 Thomas Sileo
Copyright (c) 2012 Jon Kelley (cloudfiles support + other changes)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.



./pycloudbackup.py backup -f testfile -p None 2>&1 | cut -d'=' -f3 | head -1
"""

import tarfile
import tempfile
import os
#import sys
import ConfigParser
from datetime import datetime
from getpass import getpass
import logging

import boto
from boto.s3.key import Key
import shelve
import boto.glacier
import boto.glacier.layer2
from boto.glacier.exceptions import UnexpectedHTTPResponseError
from beefish import decrypt, encrypt
import aaargh
import json

import cloudfiles

DEFAULT_LOCATION = "us-east-1"
DEFAULT_RACKSPACE_LOCATION = "dfw" # other options = ord, lon

app = aaargh.App(description="Compress, encrypt and upload files directly to Rackspace Cloudfiles/Amazon S3/Glacier.")

log = logging.getLogger(__name__)

if not log.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

config = ConfigParser.SafeConfigParser()
config.read(os.path.expanduser("~/.pycloudbackup.conf"))

class glacier_shelve(object):
    """
    Context manager for shelve
    """

    def __enter__(self):
        self.shelve = shelve.open(os.path.expanduser("~/.bakthat.db"))

        return self.shelve

    def __exit__(self, exc_type, exc_value, traceback):
        self.shelve.close()


class S3Backend:
    """
    Backend to handle S3 upload/download
    """
    def __init__(self, conf):
        if conf is None:
            try:
                access_key = config.get("aws", "access_key")
                secret_key = config.get("aws", "secret_key")
                bucket = config.get("aws", "s3_bucket")
                try:
                    region_name = config.get("aws", "region_name")
                except ConfigParser.NoOptionError:
                    region_name = DEFAULT_LOCATION
            except ConfigParser.NoOptionError:
                log.error("Configuration file not available.")
                log.info("Use 'bakthat configure' to create one.")
                return
        else:
            access_key = conf.get("access_key")
            secret_key = conf.get("secret_key")
            bucket = conf.get("bucket")
            region_name = conf.get("region_name", DEFAULT_LOCATION)

        con = boto.connect_s3(access_key, secret_key)
        if region_name == DEFAULT_LOCATION:
            region_name = ""
        self.bucket = con.create_bucket(bucket, location=region_name)
        self.container = "S3 Bucket: {}".format(bucket)

    def download(self, keyname):
        k = Key(self.bucket)
        k.key = keyname

        encrypted_out = tempfile.TemporaryFile()
        k.get_contents_to_file(encrypted_out)
        encrypted_out.seek(0)
        
        return encrypted_out

    def cb(self, complete, total):
        percent = int(complete * 100.0 / total)
        log.info("Upload completion: {}%".format(percent))

    def upload(self, keyname, filename, cb=True):
        k = Key(self.bucket)
        k.key = keyname
        upload_kwargs = {}
        if cb:
            upload_kwargs = dict(cb=self.cb, num_cb=10)
        k.set_contents_from_file(filename, **upload_kwargs)
        k.set_acl("private")

    def ls(self):
        return [key.name for key in self.bucket.get_all_keys()]

    def delete(self, keyname):
        k = Key(self.bucket)
        k.key = keyname
        self.bucket.delete_key(k)



class GlacierBackend:
    """
    Backend to handle Glacier upload/download
    """
    def __init__(self, conf):
        if conf is None:
            try:
                access_key = config.get("aws", "access_key")
                secret_key = config.get("aws", "secret_key")
                vault_name = config.get("aws", "glacier_vault")
                try:
                    region_name = config.get("aws", "region_name")
                except ConfigParser.NoOptionError:
                    region_name = DEFAULT_LOCATION
            except ConfigParser.NoOptionError:
                log.error("Configuration file not available.")
                log.info("Use 'bakthat configure' to create one.")
                return
        else:
            access_key = conf.get("access_key")
            secret_key = conf.get("secret_key")
            vault_name = conf.get("vault")
            region_name = conf.get("region_name", DEFAULT_LOCATION)

        con = boto.connect_glacier(aws_access_key_id=access_key,
                                    aws_secret_access_key=secret_key, region_name=region_name)

        self.conf = conf
        self.vault = con.create_vault(vault_name)
        self.backup_key = "bakthat_glacier_inventory"
        self.container = "Glacier vault: {}".format(vault_name)

    def backup_inventory(self):
        """
        Backup the local inventory from shelve as a json string to S3
        """
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            archives = d["archives"]

        s3_bucket = S3Backend(self.conf).bucket
        k = Key(s3_bucket)
        k.key = self.backup_key

        k.set_contents_from_string(json.dumps(archives))

        k.set_acl("private")


    def restore_inventory(self):
        """
        Restore inventory from S3 to local shelve
        """
        s3_bucket = S3Backend(self.conf).bucket
        k = Key(s3_bucket)
        k.key = self.backup_key

        loaded_archives = json.loads(k.get_contents_as_string())

        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            archives = loaded_archives
            d["archives"] = archives


    def upload(self, keyname, filename):
        archive_id = self.vault.create_archive_from_file(file_obj=filename)

        # Storing the filename => archive_id data.
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            archives = d["archives"]
            archives[keyname] = archive_id
            d["archives"] = archives

        self.backup_inventory()

    def get_archive_id(self, filename):
        """
        Get the archive_id corresponding to the filename
        """
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            archives = d["archives"]

            if filename in archives:
                return archives[filename]

        return None

    def download(self, keyname):
        """
        Initiate a Job, check its status, and download the archive if it's completed.
        """
        archive_id = self.get_archive_id(keyname)
        if not archive_id:
            return
        
        with glacier_shelve() as d:
            if not d.has_key("jobs"):
                d["jobs"] = dict()

            jobs = d["jobs"]
            job = None

            if keyname in jobs:
                # The job is already in shelve
                job_id = jobs[keyname]
                try:
                    job = self.vault.get_job(job_id)
                except UnexpectedHTTPResponseError: # Return a 404 if the job is no more available
                    del job[keyname]

            if not job:
                # Job initialization
                job = self.vault.retrieve_archive(archive_id)
                jobs[keyname] = job.id
                job_id = job.id

            # Commiting changes in shelve
            d["jobs"] = jobs

        log.info("Job {action}: {status_code} ({creation_date}/{completion_date})".format(**job.__dict__))

        if job.completed:
            log.info("Downloading...")
            encrypted_out = tempfile.TemporaryFile()
            encrypted_out.write(job.get_output().read())
            encrypted_out.seek(0)
            return encrypted_out
        else:
            log.info("Not completed yet")
            return None

    def ls(self):
        with glacier_shelve() as d:
            if not d.has_key("archives"):
                d["archives"] = dict()

            return d["archives"].keys()

    def delete(self, keyname):
        archive_id = self.get_archive_id(keyname)
        if archive_id:
            self.vault.delete_archive(archive_id)
            with glacier_shelve() as d:
                archives = d["archives"]

                if keyname in archives:
                    del archives[keyname]

                d["archives"] = archives

            self.backup_inventory()

class CloudfilesBackend:
    """
    Backend to handle Rackspace Cloudfiles integration.
        - by jon.kelley@rackspace.com
    """
    def __init__(self, conf):
        # DONE, CLOUDFILES COMPLIANT
        if conf is None:
            try:
                auth_user = config.get("cf", "apiuser")
                auth_key = config.get("cf", "apikey")
                self.container = config.get("cf", "container")

                try:
                    region_name = config.get("cf", "region_name")
                except ConfigParser.NoOptionError:
                    region_name = DEFAULT_RACKSPACE_LOCATION
            except ConfigParser.NoOptionError:
                log.error("Configuration file not available.")
                log.info("Use 'bakthat configure' to create one.")
                return
        else:
            auth_user = conf.get("apiuser")
            auth_key = conf.get("apikey")
            self.container = conf.get("container")
            region_name = conf.get("region_name", DEFAULT_RACKSPACE_LOCATION)
            

        if  region_name == "dfw" or region_name == "ord":
            self.con = cloudfiles.get_connection(auth_user, auth_key,
                                                 authurl = "https://identity.api.rackspacecloud.com/v1.0/")
        else:
            self.con = cloudfiles.get_connection(auth_user, auth_key, 
                                                 authurl = "https://lon.identity.api.rackspacecloud.com/v1.0/")

    def download(self, keyname):
        """ Refactor complete! """
        container = self.con.create_container(self.container)
        obj = container.get_object(keyname)
        
        encrypted_out = tempfile.TemporaryFile()
        obj.read(buffer=encrypted_out)

        encrypted_out.seek(0)
        return encrypted_out


#    def cb(self, complete, total):
        #?????????????
#        percent = int(complete * 100.0 / total)
#        log.info("Upload completion: {}%".format(percent))
#        print "unimplimented"

    def upload(self, keyname, filename, cb=False):
        """ Refactor complete! """
        container = self.con.create_container(self.container)

        o = container.create_object(keyname)
        o.write(filename)

    def ls(self):
        """ Refactor complete! """
        #{u'bytes': 25605, u'last_modified': u'2012-11-29T14:47:32.365100',
        # u'hash': u'3feb7b99ab4033e378a387d4c530d7aa',\
        # u'name': u'bakthat20121129084730.tgz', u'content_type': u'application/octet-stream'}

        return [key['name'] for key in self.con[self.container].list_objects_info(limit=10000)] 

    def md5(self, keyname):
        """ Refactor complete! """
        container = self.con.create_container(self.container)
        obj = container.compute_md5sum(keyname)
        
        encrypted_out = tempfile.TemporaryFile()
        obj.read(buffer=encrypted_out)

        encrypted_out.seek(0)
        return encrypted_out

    def delete(self, keyname):
        container = self.con.create_container(self.container)
        container.delete_object(keyname)

storage_backends = dict(s3=S3Backend, glacier=GlacierBackend, cloudfiles=CloudfilesBackend)

@app.cmd(help="Backup a file or a directory, backup the current directory if no arg is provided.")
@app.cmd_arg('-f', '--filename', type=str, default=os.getcwd())
@app.cmd_arg('-d', '--destination', type=str, default="cloudfiles", help="s3|glacier|cloudfiles")
@app.cmd_arg('-p', '--password', type=str, default=None, help="Provide password non interactively.") # jonk nov 29 2012
def backup(filename, destination="cloudfiles", **kwargs):
    conf = kwargs.get("conf", None)
    storage_backend = storage_backends[destination](conf)


    arcname = filename.split("/")[-1]
    #stored_filename = arcname + datetime.now().strftime("%Y%m%d%H%M%S") + ".tgz"
    # filename file name date
    stored_filename = arcname + ".tgz"
    log.info("Backup started localname=" + filename + " remotename=" + str(stored_filename))
    password = kwargs.get("password")

    if conf is not None: # If the conf has been populated by using this as a module, set the password.
	    password = conf.get("crypto_password")
    else:
        if not password:
            password = getpass("Password (blank to disable encryption): ")

    log.info("Compressing...")
    out = tempfile.TemporaryFile()
#    with tarfile.open(fileobj=out, mode="w:gz") as tar:
#        tar.add(filename, arcname=arcname)

    tarz = tarfile.open(fileobj=out, mode="w:gz")
    tarz.add(filename, arcname=arcname)
    tarz.close()

    if password == "None" or password == "none":
        password = None

    if password:
        log.info("Encrypting...")
        encrypted_out = tempfile.TemporaryFile()
        encrypt(out, encrypted_out, password)
        stored_filename += ".enc"
        out = encrypted_out

    log.info("Uploading...")
    out.seek(0)
    storage_backend.upload(stored_filename, out)



@app.cmd(help="Set S3/Glacier/Cloudfiles credentials.")
def configure():
    configurechoice = input("What storage engine do you want to configure this to use?\n1. Rackspace Cloud Files\n2. Amazon AWS/Glacier\nEnter number: ")
    if configurechoice == 1:
        config.add_section("cf")
        config.set("cf", "apiuser", raw_input("Cloudfiles User: "))
        config.set("cf", "apikey", raw_input("Cloudfiles Key: "))
        config.set("cf", "container", raw_input("Cloudfiles Container: "))
        region_name = raw_input("Region Name (" + DEFAULT_RACKSPACE_LOCATION + "): ")
        if not region_name:
            region_name = DEFAULT_RACKSPACE_LOCATION
        config.set("cf", "region_name", region_name)

    elif configurechoice == 2:
        config.add_section("aws")
        config.set("aws", "access_key", raw_input("AWS Access Key: "))
        config.set("aws", "secret_key", raw_input("AWS Secret Key: "))
        config.set("aws", "s3_bucket", raw_input("S3 Bucket Name: "))
        config.set("aws", "glacier_vault", raw_input("Glacier Vault Name: "))
        region_name = raw_input("Region Name (" + DEFAULT_LOCATION + "): ")
        if not region_name:
            region_name = DEFAULT_LOCATION
        config.set("aws", "region_name", region_name)

    config.write(open(os.path.expanduser("~/.pycloudbackup.conf"), "w"))
    log.info("Config written in %s" % os.path.expanduser("~/.pycloudbackup.conf"))



@app.cmd(help="Restore backup in the current directory.")
@app.cmd_arg('-f', '--filename', type=str, default="")
@app.cmd_arg('-d', '--destination', type=str, default="cloudfiles", help="s3|glacier|cloudfiles")
@app.cmd_arg('-p', '--password', type=str, default=None, help="Provide password non interactively.") # jonk nov 29 2012
def restore(filename, destination="cloudfiles", **kwargs):
    conf = kwargs.get("conf", None)

    storage_backend = storage_backends[destination](conf)

    if not filename:
        log.error("No file to restore, use -f to specify one.")
        return

    keys = [name for name in storage_backend.ls() if name.startswith(filename)]
    if not keys:
        log.error("No file matched.")
        return

    key_name = sorted(keys, reverse=True)[0]
    log.info("Restoring " + key_name)

    # Asking password before actually download to avoid waiting
    if key_name and key_name.endswith(".enc"):
        password = kwargs.get("password")
        if not password:
            password = getpass()
        elif password == "None":
            password = None

    log.info("Downloading...")
    out = storage_backend.download(key_name)

    if out and key_name.endswith(".enc"):
        log.info("Decrypting...")
        decrypted_out = tempfile.TemporaryFile()
        decrypt(out, decrypted_out, password)
        out = decrypted_out
        log.info( "Decrypt Filehandler " + str(out))

    if out:
        log.info("Uncompressing...")
        out.seek(0)
        tar = tarfile.open(fileobj=out)
        tar.extractall()
        tar.close()


@app.cmd(help="Delete a backup.")
@app.cmd_arg('-f', '--filename', type=str, default="")
@app.cmd_arg('-d', '--destination', type=str, default="cloudfiles", help="s3|glacier|cloudfiles")
def delete(filename, destination="cloudfiles", **kwargs):
    conf = kwargs.get("conf", None)
    storage_backend = storage_backends[destination](conf)

    if not filename:
        log.error("No file to delete, use -f to specify one.")
        return

    keys = [name for name in storage_backend.ls() if name.startswith(filename)]
    if not keys:
        log.error("No file matched.")
        return

    key_name = sorted(keys, reverse=True)[0]
    log.info("Deleting " + key_name)

    storage_backend.delete(key_name)


@app.cmd(help="List stored backups.")
@app.cmd_arg('-d', '--destination', type=str, default="cloudfiles", help="s3|glacier|cloudfiles")
def ls(destination="cloudfiles", **kwargs):
    conf = kwargs.get("conf", None)
    storage_backend = storage_backends[destination](conf)
    
    log.info(storage_backend.container)

    for filename in storage_backend.ls():
        log.info(filename)

@app.cmd(help="Get an md5 of backup.")
@app.cmd_arg('-f', '--filename', type=str, default="")
@app.cmd_arg('-d', '--destination', type=str, default="cloudfiles", help="cloudfiles")
def md5(filename, destination="cloudfiles", **kwargs):
    # Only supports cloudfiles, sorry AWS! I dunno how!
    conf = kwargs.get("conf", None)
    storage_backend = storage_backends[destination](conf)

    if not filename:
        log.error("No file to md5, use -f to specify one.")
        return

    md5=storage_backend.md5()
    log.info( str(filename) + " : " + str(md5))
    return md5

@app.cmd(help="Backup Glacier inventory to S3")
def backup_glacier_inventory(**kwargs):
    conf = kwargs.get("conf", None)
    glacier_backend = GlacierBackend(conf)
    glacier_backend.backup_inventory()


@app.cmd(help="Restore Glacier inventory from S3")
def restore_glacier_inventory(**kwargs):
    conf = kwargs.get("conf", None)
    glacier_backend = GlacierBackend(conf)
    glacier_backend.restore_inventory()


def main():
    app.run()

if __name__ == '__main__':
    main()
