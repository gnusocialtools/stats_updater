#!/usr/bin/python3
# -*- coding: utf-8 -*-

import configparser
import json
import os
import pymysql
import urllib.request

from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import text

# Path to GSTools website.
GSTOOLS_WEBSITE = "/data/Projects/github/gstools/gstools-website/"

class Updater:
    def __init__(self):
        self.data = {}
        self.config = configparser.ConfigParser()
        self.instances = []
        
        self.get_database_config()
        self.connect_to_database()
        self.get_data_from_db()
        self.retrieve_instances_data()
        self.parse_and_insert()
        
    def get_database_config(self):
        """
        Gets database configuration from GSTools website.
        """
        configs_path = os.path.join(GSTOOLS_WEBSITE, "config")
        for item in os.listdir(configs_path):
            if item.endswith("conf"):
                self.config.read(os.path.join(configs_path, item))
                
        self.config = self.config["database"]
                
    def connect_to_database(self):
        """
        Connects to database. Nothing more.
        """
        dbtype = self.config["type"]
        username = self.config["user"]
        password = self.config["password"]
        hostname = self.config["host"]
        port = self.config["port"]
        database = self.config["database"]
        self.database = "{0}://{1}:{2}@{3}:{4}/{5}?charset=utf8".format(dbtype, username, password, hostname, port, database)
        try:
            self.database = create_engine(self.database)
            self.database = self.database.connect()
            print("Connection to database established")
        except exc.OperationalError as e:
            print("Failed to connect to database: {0}".format(e))
            
    def get_data_from_db(self):
        """
        Get instances addresses from database.
        """
        self.instances = self.database.execute("SELECT id, address FROM sites").fetchall()
        print("Got {0} addresses".format(len(self.instances)))
        
        self.users = self.database.execute("SELECT * FROM remote_users").fetchall()
        self.groups = self.database.execute("SELECT * FROM remote_groups").fetchall()
        self.plugins = self.database.execute("SELECT * FROM remote_plugins").fetchall()
        
        # These queries might return empty results, especially
        # on first start. So make them a lists with one empty dict.
        if not self.users:
            self.users = [{"id": -1, "sites_id": -1, "remote_id": -1, "username": None, "fullname": None}]
        if not self.groups:
            self.groups = [{"id": -1, "sites_id": -1, "remote_gid": -1, "name": None}]
        if not self.plugins:
            self.plugins = [{"id": -1, "sites_id": -1, "name": None, "version": None, "homepage": None}]
        
    def retrieve_instances_data(self):
        """
        Retrieve instances data.
        """
        curitem = 1
        for item in self.instances:
            # Just for ease of use, we will use only instance
            # address.
            item = item[1]
            # Item should contain HTTP or HTTPS. Otherwise add
            # it.
            if not "http" in item or not "https" in item:
                print("[{0}/{1}] Address '{2}' do not contain HTTP or HTTPS, adding HTTP as default".format(curitem, len(self.instances), item))
                item = "http://" + item
            print("[{0}/{1}] Retrieving stats for '{2}'".format(curitem, len(self.instances), item))
            try:
                filename, headers = urllib.request.urlretrieve(item + "/main/statistics", filename = "/tmp/gstools_stats")
            except urllib.error.URLError as e:
                print("[{0}/{1}] Failed to retrieve data from '{2}': {3}".format(curitem, len(self.instances), item, e))
                continue

            # Load retrieved data into dictionary for later parse.
            data = json.loads(open("/tmp/gstools_stats", "r").read())
            self.data[item] = data
            os.remove("/tmp/gstools_stats")
            
            curitem += 1
            
        print("Data received.")
        
    def parse_and_insert(self):
        """
        Parse and insert/update instance's data.
        """
        for site in self.data:
            print("Inserting data for '{0}'...".format(site))
            # Obtain site ID from database.
            site_id = self.database.execute(text("SELECT id FROM sites WHERE address=:site"), {"site": site}).fetchone()["id"]
            # Here we go, the magic begins.
            # We will commit to database only one site at time.
            c = self.database.begin()
            # Inserting users.
            for user in self.data[site]["users"]:
                # Create a copy of dict with data, because we should
                # insert one item. If we won't copy it, we will get
                # an error about changing dict size in runtime.
                data = self.data[site]["users"][user]
                data["sites_id"] = site_id
                # Fullname can be empty. In that case we will use
                # nickname as fullname.
                if not data["fullname"]:
                    data["fullname"] = data["nickname"]
                # Check if we already got this user and execute
                # approriate query.
                to_update = False
                for item in self.users:
                    if item["sites_id"] == site_id and item["username"] == data["nickname"] and item["remote_uid"] == int(data["id"]):
                        data["local_id"] = item["id"]
                        to_update = True
                        break
                        
                if not to_update:
                    self.database.execute(text("INSERT INTO remote_users (sites_id, remote_uid, username, fullname) VALUES (:sites_id, :id, :nickname, :fullname)"), data)
                else:
                    self.database.execute(text("UPDATE remote_users SET sites_id=:sites_id, remote_uid=:id, username=:nickname, fullname=:fullname WHERE id=:local_id"), data)
            
            # Inserting groups.
            for group in self.data[site]["groups"]:
                # Create a copy of dict with data. Same as above.
                data = self.data[site]["groups"][group]
                data["sites_id"] = site_id
                # Check if we already got this group.
                to_update = False
                for item in self.groups:
                    if item["sites_id"] == site_id and data["name"] == item["name"]:
                        data["local_id"] = item["id"]
                        to_update = True
                        break
                
                if not to_update:
                    self.database.execute(text("INSERT INTO remote_groups (sites_id, remote_gid, name) VALUES (:sites_id, :id, :name)"), data)
                else:
                    self.database.execute(text("UPDATE remote_groups SET sites_id=:sites_id, remote_gid=:id, name=:name WHERE id=:local_id"), data)
                    
            # Inserting plugins data.
            for plugin in self.data[site]["plugins"]:
                # Copy of dict. Yes, again.
                data = self.data[site]["plugins"][plugin]
                data["sites_id"] = site_id
                # Insert "No homepage" in no homepage parsed.
                if not data["homepage"]:
                    data["homepage"] = "No homepage"
                # Check if we already got this plugin.
                to_update = False
                for item in self.plugins:
                    if item["sites_id"] == site_id and data["name"] == item["name"] and data["version"] == item["version"]:
                        data["local_id"] = item["id"]
                        to_update = True
                        break
                    
                if not to_update:
                    self.database.execute(text("INSERT INTO remote_plugins (sites_id, name, version, homepage) VALUES (:sites_id, :name, :version, :homepage)"), data)
                else:
                    self.database.execute(text("UPDATE remote_plugins SET sites_id=:sites_id, name=:name, version=:version, homepage=:homepage WHERE id=:local_id"), data)
                
            # Commit changes.
            c.commit()
            
if __name__ == "__main__":
    Updater()
