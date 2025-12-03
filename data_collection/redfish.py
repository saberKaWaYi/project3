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

handler=get_rotating_handler("redfish.log")
logging_redfish=logging.getLogger("redfish")
logging_redfish.setLevel(logging.INFO)
logging_redfish.addHandler(handler)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import requests
from requests.auth import HTTPBasicAuth

class Basic:

    def __init__(self,ip,username,password):
        self.ip=ip
        self.username=username
        self.password=password
        self.base_url=f"https://{self.ip}/redfish/v1"
        self.session=requests.Session();self.session.verify=False
        self.session.auth=HTTPBasicAuth(self.username,self.password)
        self.session.headers.update({'Content-Type':'application/json'})
        self.is_authenticated=self.check_authentication()

    def check_authentication(self):
        try:
            response=self.session.get(f"{self.base_url}",timeout=30)
            if response.status_code==200:
                return True
            else:
                logging_redfish.error("="*50+f"\n{self.ip}\n登陆不上\n"+"="*50)
                return False
        except:
            logging_redfish.error("="*50+f"\n{self.ip}\n网络问题\n"+"="*50)
            return False

    def logout(self):
        if self.session:
            self.session.close()
            self.session=None

    def __enter__(self):
        return self

    def __exit__(self,exc_type,exc_val,exc_tb):
        self.logout()
        return False

class Dell(Basic):

    def __init__(self,ip,username,password):
        super().__init__(ip,username,password)

    def get_psu_detail(self):
        if not self.check_authentication():
            return [[],[],[]]
        url=f"{self.base_url}/Chassis/System.Embedded.1/Power"
        try:
            response=self.session.get(
                url,
                timeout=5
            )
            lt=response.json()["PowerSupplies"]
            result=[[],[],[]]
            for i in lt:
                if not i["PowerInputWatts"]:
                    continue
                result[2].append(i["PowerInputWatts"])
                if not i["LineInputVoltage"]:
                    continue
                result[0].append(i["LineInputVoltage"])
                result[1].append(i["PowerInputWatts"]/i["LineInputVoltage"])
            return result
        except Exception as e:
            logging_redfish.error("="*50+"\n"+self.ip+"\n"+str(e)+"\n"+"="*50)
            return  [[],[],[]]
        
class Huawei(Basic):

    def get_psu_detail(self):
        if not self.check_authentication():
            return [[],[],[]]
        url=url=f"{self.base_url}/Chassis/Enc/Power"
        try:
            response=self.session.get(
                url,
                timeout=5
            )
            temp=response.json()["PowerControl"][0]["PowerConsumedWatts"]
            return [[],[],[temp]]
        except:
            pass
        url=url=f"{self.base_url}/Chassis/Enclosure/Power"
        try:
            response=self.session.get(
                url,
                timeout=5
            )
            temp=response.json()["PowerControl"][0]["PowerConsumedWatts"]
            return [[],[],[temp]]
        except:
            pass
        url=f"{self.base_url}/Chassis"
        try:
            response=self.session.get(
                url,
                timeout=5
            )
            temp=response.json()["Members"][0]["@odata.id"].split("/")[-1]
        except Exception as e:
            logging_redfish.error("="*50+"\n"+self.ip+"\n"+str(e)+"\n"+"="*50)
            return  [[],[],[]]
        url=url+"/"+temp+"/"+"ThresholdSensors"
        try:
            response=self.session.get(
                url,
                timeout=5
            )
            lt=response.json()["Sensors"]
            result=[[],[],[]]
            for i in lt:
                if i["Name"]!="Power":
                    continue
                result[-1].append(i["ReadingValue"])
            return result
        except Exception as e:
            logging_redfish.error("="*50+"\n"+self.ip+"\n"+str(e)+"\n"+"="*50)
            return  [[],[],[]]