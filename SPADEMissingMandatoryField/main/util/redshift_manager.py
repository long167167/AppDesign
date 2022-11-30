#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import psycopg2
import pandas as pd
import sys

from .aws_secret_manager import get_redshift_credentials


class RedshiftManager:

    def __init__(self, region_name):
        self.region_name = region_name

    def connect(self):
        print ('Conecting to RedShift...')
        redshift_credendials = get_redshift_credentials(self.region_name)
        redshift_config = json.loads(redshift_credendials)

        print(redshift_config)
        DATABASE = redshift_config['dbname']
        USER = redshift_config['username']
        PASSWORD = redshift_config['password']
        HOST = redshift_config['host']
        PORT = redshift_config['port']

        try:
            conn = psycopg2.connect(host=HOST, user=USER,
                                    password=PASSWORD, port=PORT,
                                    database=DATABASE)
        except Exception as err:
            print('Error while conneting to RefShift')
            print(err)
            sys.exit(1)
        print ('Successfully connected to RedShift.')

        self.conn = conn
        self.cur = conn.cursor()

    def query_db(self, query, one=False):
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data
        
    def close_connection(self):
        self.cur.connection.close()