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
                        
            sql = "COPY "+table[0]+ " TO STDOUT WITH HEADER CSV"
            self.log.info('SQL: %s', sql)
          
        
            filename = "_"+table[0]+".csv"
           
            self.hook.copy_expert(sql, self.folder+'/'+filename)
        
   
        for output in self.hook.conn.notices:
            self.log.info(output)
