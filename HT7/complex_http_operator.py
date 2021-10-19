from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook

from airflow.exceptions import AirflowException

import os
import json


class ComplexHttpOperator(SimpleHttpOperator):
    def __init__(self, save_on_disk=False, *args, **kwargs):
        super(ComplexHttpOperator, self).__init__( *args, **kwargs)
        self.save_on_disk = save_on_disk
        
    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)

        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        
        if self.save_on_disk:
            process_date =self.data['date']
            os.makedirs(os.path.join('/','home','user','api_data',process_date),exist_ok=True)  
            with open(os.path.join('/','home','user','api_data',process_date,process_date+'.json'),'w') as json_file:
                self.log.info('Writing data from API to /home/user/api_data/'+process_date)
                data = response.json()
                json.dump(data, json_file)
        if self.xcom_push_flag:
            return response.text
        