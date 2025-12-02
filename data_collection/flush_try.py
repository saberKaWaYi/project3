import os

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

import logging
from logging.handlers import RotatingFileHandler

def get_rotating_handler(filename,max_bytes=1024*1024*1024,backup_count=5):
    handler=RotatingFileHandler(
        os.path.join(log_dir,filename),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    formatter=logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s'
    )
    handler.setFormatter(formatter)
    return handler

handler=get_rotating_handler("flush_try.log")
logging_flush_try=logging.getLogger("flush_try")
logging_flush_try.setLevel(logging.INFO)
logging_flush_try.addHandler(handler)

from connect import Connect_Mysql,Connect_Mongodb
from bson import ObjectId
import pandas as pd
from redfish import Basic
from concurrent.futures import ThreadPoolExecutor,as_completed

class Run:

    def __init__(self,config1,config2):
        self.config1=config1
        self.config2=config2
        self.db_mysql=Connect_Mysql(config1)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.db_mongo=Connect_Mongodb(config2)
        self.db_mongo_client=self.db_mongo.client
        self.pipeline=[
            {
                '$match':{
                    'status':1,
                    'asset_status':{
                        '$in':[
                            ObjectId("5f964e31df0dfd65aaa716ec"),
                            ObjectId("5fcef6de94103c791bc2a471")
                        ]
                    }
                }
            },
            {
                '$lookup':{
                    'from':'cds_ci_location_detail',
                    'localField':'_id',
                    'foreignField':'device_id',
                    'as':'location'
                }
            },
            {
                '$match':{
                    'location.status':1
                }
            },
            {
                '$project':{
                    "out_band_ip":1,
                    "web_brand":1,
                    "hostname":1
                }
            }
        ]
        self.data=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_server.aggregate(self.pipeline))).astype(str)[["hostname","out_band_ip","web_brand"]].values.tolist()
        self.correct_data=set(self.db_mysql.get_table_data("","select hostname from hardware.correct_up")["hostname"].values.tolist())
        self.result1=[]
        self.result2=[]

    def truncate_table(self):
        sql='''
        truncate table hardware.uncorrect_up;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()

    def fc(self,hostname,ip,brand):
        hostname="-".join([i.strip() for i in hostname.split("-")])
        if hostname.lower()=="none" or hostname.lower()=="null" or hostname.lower()=="nan" or hostname=="" or hostname=="-" or hostname=="--" or hostname=="---" or hostname==None:
            return
        if hostname in self.correct_data:
            return
        ip=".".join([i.strip() for i in ip.split(".")])
        if "." not in ip:
            return
        brand=brand.lower()
        if brand=="dell inc.":
            m=Basic(ip,"root","P@$$w0rd")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"root","P@$$w0rd"))
                m.logout()
                return
            m.logout()
            m=Basic(ip,"root","calvin")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"root","calvin"))
                m.logout()
                return
            m.logout()
            self.result2.append((hostname,ip,brand))
        elif brand=="inspur":
            m=Basic(ip,"admin","P@$$w0rd")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"admin","P@$$w0rd"))
                m.logout()
                return
            m.logout()
            m=Basic(ip,"admin","admin")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"admin","admin"))
                m.logout()
                return
            m.logout()
            self.result2.append((hostname,ip,brand))
        elif brand=="huawei":
            m=Basic(ip,"Administrator","Huawei@storage")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"Administrator","Huawei@storage"))
                m.logout()
                return
            m.logout()
            self.result2.append((hostname,ip,brand))
        elif brand=="supermicro":
            m=Basic(ip,"ADMIN","ADMIN@123")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"ADMIN","ADMIN@123"))
                m.logout()
                return
            m.logout()
            m=Basic(ip,"admin","admin")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"admin","admin"))
                m.logout()
                return
            m.logout()
            self.result2.append((hostname,ip,brand))
        elif brand=="lenovo":
            m=Basic(ip,"ADMIN","ADMIN@123456")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"ADMIN","ADMIN@123456"))
                m.logout()
                return
            m.logout()
            self.result2.append((hostname,ip,brand))
        else:
            m=Basic(ip,"ADMIN","ADMIN@123")
            if m.is_authenticated:
                self.result1.append((hostname,ip,brand,"ADMIN","ADMIN@123"))
                m.logout()
                return
            m.logout()
            self.result2.append((hostname,ip,brand))

    def collect(self):
        with ThreadPoolExecutor(max_workers=50) as executor:
            pool=[]
            for i in self.data:
                pool.append(executor.submit(self.fc,i[0],i[1],i[2]))
            for task in as_completed(pool):
                task.result()

    def insert_data(self):
        sql='''
        insert into hardware.correct_up (hostname,ip,brand,username,password) values (%s,%s,%s,%s,%s);
        '''
        self.db_mysql_client.executemany(sql,self.result1)
        self.db_mysql.client.commit()
        sql='''
        insert into hardware.uncorrect_up (hostname,ip,brand) values (%s,%s,%s);
        '''
        self.db_mysql_client.executemany(sql,self.result2)
        self.db_mysql.client.commit()

    def run(self):
        self.truncate_table()
        self.collect()
        self.insert_data()

if __name__=="__main__":
    config1={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    config2={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config1,config2)
    m.run()