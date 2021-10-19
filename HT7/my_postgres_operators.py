# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import csv
import os
class myPostgresOperator_ListToCSV(BaseOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, folder, filename,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            *args, **kwargs):
        super(myPostgresOperator_ListToCSV, self).__init__(*args, **kwargs)
        self.sql = sql
        self.filename = filename
        self.folder = folder
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.log.info('Executing: %s', self.sql)

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        records = self.hook.get_records(self.sql, parameters=self.parameters)
        tables=[]
        for rec in records:
            tables.append(rec)
        os.makedirs(self.folder ,exist_ok=True)  
        file = open((self.folder+'/'+self.filename),'w+',newline='')
        with file:
            write = csv.writer(file)
            write.writerows(tables)

        for output in self.hook.conn.notices:
            self.log.info(output)

class myPostgresOperator_TablesToCSV(BaseOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,  folder, tables_csv_filename, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            *args, **kwargs):
        super(myPostgresOperator_TablesToCSV, self).__init__(*args, **kwargs)
        self.sql=sql
        self.tables_csv_filename = tables_csv_filename
        self.folder = folder
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.log.info('Executing: %s', self.sql)

    def execute(self, context):
        
        file = open(self.tables_csv_filename,newline='')
        tables =[]
        with file:
            reader = csv.reader(file)
            for row in reader:
                tables.append(row)
        self.log.info('Tales to download: %s', tables)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        for table in tables:
            sql_cols="SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '"+table[0]+"'"
            cols = self.hook.get_records(sql_cols, parameters=self.parameters)
            columns=[]
            for col in cols:
                columns.append(col[0])
            
            sql = "SELECT * FROM "+table[0]
            self.log.info('SQL: %s', sql)
            records = self.hook.get_records(sql, parameters=self.parameters)
        
            #Write to csv file
            filename = "_"+table[0]+".csv"
            with open(self.folder+'/'+filename, 'w') as fp:
                a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = '|')
                a.writerow(columns)
                a.writerows(records)
   
        for output in self.hook.conn.notices:
            self.log.info(output)