import sys
import os
import time
import yaml
import pendulum
from typing import Dict, Any, Union
from pendulum.date import Date


sys.setrecursionlimit(200)

class CloudAceConfigUtilsYaml:
    def __init__(self, config_path: str) -> None:
        self.config_path = config_path
        self.config_body = self._parse_config()

    def _parse_config(self) -> Dict[str, Any]:
        # check_file = os.path.isfile(self.config_path)
        # if check_file:
        #     config = open(self.config_path, 'r')
        #     config_body = yaml.load(config, Loader=yaml.FullLoader) 
        #     config.close()
        # else:
        #     config_body = self._parse_config()
        # return config_body
        config = open(self.config_path, 'r')
        config_body = yaml.load(config, Loader=yaml.FullLoader) 
        config.close()
        return config_body

    def get_start_date(self) -> Date:
        return pendulum.parse(
            self.config_body['start_date'],
            tz=self.config_body['default_args']['timezone']
        )

    def get_schedule_interval(self) -> Union[str, None]:
        schedule_interval = None
        if 'schedule_interval' in self.config_body.keys() and self.config_body['schedule_interval'] != 'None':
            schedule_interval = self.config_body['schedule_interval']
        return schedule_interval

    def get_path(self, path_type: str, add_date_path: bool = False) -> str:
        defined_path = self.config_body[path_type] + "/" if self.config_body[path_type] != "" else self.config_body[path_type]
        if add_date_path:
            defined_path += "{{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
        return defined_path
