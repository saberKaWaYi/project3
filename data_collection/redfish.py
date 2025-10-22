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
import atexit

class Basic:

    def __init__(self,idrac_ip,username,password):
        self.idrac_ip=idrac_ip
        self.username=username
        self.password=password
        self.base_url=f"https://{self.idrac_ip}/redfish/v1"
        self.session=requests.Session();self.session.verify=False
        self.session_id=None
        self.error_reason=None
        atexit.register(self.logout)
        self.login()

    def login(self):
        login_url=f"{self.base_url}/SessionService/Sessions"
        data={
            "UserName":self.username,
            "Password":self.password
        }
        try:
            response=self.session.post(
                login_url,
                json=data,
                verify=False,
                timeout=5
            )
            if response.status_code==201:
                self.session_id=response.headers['Location'].split('/')[-1]
                self.session.headers.update({
                    'X-Auth-Token':response.headers.get('X-Auth-Token'),
                    'Content-Type':'application/json'
                })
            elif response.status_code==401:
                self.error_reason="="*50+f"\n{self.idrac_ip}密码不对。\n"+"="*50
                logging_redfish.error(self.error_reason)
            else:
                self.error_reason="="*50+f"\n{self.idrac_ip}未知状态码。\n"+"="*50
                logging_redfish.error(self.error_reason)
        except Exception as e:
            self.error_reason="="*50+f"\n{self.idrac_ip}网络不通。\n"+str(e)+"="*50
            logging_redfish.error(self.error_reason)

    def test(self):
        print(self.session_id)

    def logout(self):
        if not self.session_id:
            return
        try:
            logout_url=f"{self.base_url}/SessionService/Sessions/{self.session_id}"
            self.session.delete(logout_url,timeout=5)
            self.session.close()
            self.session_id=None
        except:
            logging_redfish.error(f"{self.idrac_ip}退出失败。")

class Dell(Basic):

    def get_psu_detail(self):
        if not self.session_id:
            return [[],[],[]]
        url=f"{self.base_url}/Chassis/System.Embedded.1/Power"
        try:
            response=self.session.get(
                url,
                verify=False,
                timeout=5
            )
            lt=response.json()["PowerSupplies"]
            result=[[],[],[]]
            for i in lt:
                result[0].append(i["LineInputVoltage"])
                result[1].append(i["PowerInputWatts"]/i["LineInputVoltage"])
                result[2].append(i["PowerInputWatts"])
            return result
        except Exception as e:
            logging_redfish.error("="*50+"\n"+self.idrac_ip+"\n"+str(e)+"\n"+"="*50)
            return  [[],[],[]]
        
class Huawei(Basic):

    def get_psu_detail(self):
        if not self.session_id:
            return [[],[],[]]
        url=url=f"{self.base_url}/Chassis/Enclosure/Power"
        try:
            response=self.session.get(
                url,
                verify=False,
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
                verify=False,
                timeout=5
            )
            temp=response.json()["Members"][0]["@odata.id"].split("/")[-1]
        except:
            logging_redfish.error("="*50+"\n"+self.idrac_ip+"\n"+str(e)+"\n"+"="*50)
            return  [[],[],[]]
        url=url+"/"+temp+"/"+"ThresholdSensors"
        try:
            response=self.session.get(
                url,
                verify=False,
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
            logging_redfish.error("="*50+"\n"+self.idrac_ip+"\n"+str(e)+"\n"+"="*50)
            return  [[],[],[]]
        
if __name__=="__main__":
    m=Huawei("10.212.122.37","Administrator","Huawei@storage")
    print(m.get_psu_detail())