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

handler=get_rotating_handler("error.log")
logging_error=logging.getLogger("error")
logging_error.setLevel(logging.INFO)
logging_error.addHandler(handler)

handler=get_rotating_handler("monitor.log")
logging_monitor=logging.getLogger("monitor")
logging_monitor.setLevel(logging.INFO)
logging_monitor.addHandler(handler)

import time
from get_info import *
from datetime import datetime
from connect import Connect_Mysql,Connect_Clickhouse
from queue import Queue
import threading
from concurrent.futures import ThreadPoolExecutor,as_completed
import subprocess
from redfish import Dell,Huawei

class Run:

    def __init__(self,config1,config2,config3):
        self.config1=config1
        self.config2=config2
        self.config3=config3
        self.zd1=get_relationship(self.config1,get_ObjectId(self.config1,"庆阳"))
        self.time_=datetime.now()
        u_p_list=Connect_Mysql(self.config2).get_table_data("","select ip,username,password from power.server_username_and_password")
        self.zd2=dict(zip(u_p_list["ip"].values.tolist(),u_p_list[["username","password"]].values.tolist()))
        self.flag=False
        self.tasks1=Queue();self.count1=0;self.lockc1=threading.Lock()
        self.tasks2=Queue();self.count2=0;self.lockc2=threading.Lock()
        self.tasks3=Queue()
        self.task_pool=[];self.lock1=threading.Lock();self.count3=0
        self.tasks4=Queue();self.count4=0;self.lockc3=threading.Lock()
        self.result=[];self.lock2=threading.Lock()

    def run(self):
        t1=threading.Thread(target=self.post_main)
        t2=threading.Thread(target=self.process_task1_main)
        t3=threading.Thread(target=self.process_task2_main)
        t4=threading.Thread(target=self.post_else)
        t5=threading.Thread(target=self.process_else)
        t6=threading.Thread(target=self.process_task4_main)
        t7=threading.Thread(target=self.monitor)
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t5.start()
        t6.start()
        t7.start()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
        t6.join()
        t7.join()
        conn=Connect_Clickhouse(self.config3)
        client=conn.client
        insert_sql="""
        INSERT INTO power.power_data
        (city, data_center, room, rack, hostname, ts, voltage, current, power, ip, brand, type)
        VALUES
        """
        values=[]
        for item in self.result:
            values.append(
                f"('{item['city']}', '{item['data_center']}', '{item['room']}', '{item['rack']}', "
                f"'{item['hostname']}', '{item['ts'].strftime('%Y-%m-%d %H:%M:%S')}', "
                f"{item['voltage']}, {item['current']}, {item['power']}, "
                f"'{item['ip']}', '{item['brand']}', '{item['type']}')"
            )
        insert_sql+=",".join(values)
        client.execute(insert_sql)

    def get_zd(self,hostname,ip,brand,type_,rack):
        temp_zd={}
        hostname="-".join([i.strip() for i in hostname.split("-")]);ip=".".join([i.strip() for i in ip.split(".")]);brand=brand.lower()
        temp_zd["hostname"]=hostname;temp_zd["ip"]=ip;temp_zd["brand"]=brand;temp_zd["type"]=type_
        temp_lt=rack.split("|")
        temp_zd["city"]=temp_lt[0];temp_zd["data_center"]=temp_lt[1];temp_zd["room"]=temp_lt[2];temp_zd["rack"]=temp_lt[3]
        temp_zd["ts"]=self.time_
        return temp_zd
    
    def filter(self,hostname,ip):
        if hostname.lower()=="none" or hostname.lower()=="null" or hostname.lower()=="nan" or hostname=="" or hostname=="-" or hostname=="--" or hostname=="---" or hostname==None:
            return False
        if "." not in ip:
            return False
        return True

    def post_server(self,hostname,ip,brand,type_,rack):
        zd=self.get_zd(hostname,ip,brand,type_,rack)
        if not self.filter(zd["hostname"],zd["ip"]):
            return
        if zd["brand"]=="supermicro":
            self.tasks1.put(zd)
        elif zd["brand"]=="dell inc." or zd["brand"]=="huawei":
            if zd["ip"] in self.zd2:
                self.tasks2.put(zd)
            else:
                logging_error.error("="*50)
                logging_error.error(zd)
                logging_error.error("账号以及密码缺失。")
                logging_error.error("="*50)
        else:
            self.tasks3.put(zd)

    def post_network(self,hostname,ip,brand,type_,rack):
        zd=self.get_zd(hostname,ip,brand,type_,rack)
        if not self.filter(zd["hostname"],zd["ip"]):
            return
        if zd["brand"] not in ["huawei","huarong"]:
            return
        self.tasks4.put(zd)

    def snmpwalk_server(self,ip,oid):
        try:
            command=f"snmpwalk -v 2c -c public {ip} {oid}"
            process=subprocess.run(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=10
            )
            temp=[]
            for line in process.stdout.strip().split("\n"):
                s=line[line.index(":")+1:].strip().strip("\"")
                temp.append(eval(s))
            return temp
        except:
            logging_error.error(command)
            return []
        
    def demo(self,lt1,lt2,lt3):
        a=sum(lt1)/len(lt1) if len(lt1) else 0;b=sum(lt2);c=sum(lt3)
        c=max(c,a*b)
        a=a if a else c/b if b else 0
        b=b if b else c/a if a else 0
        return [round(a,2),round(b,2),round(c,2)]

    def process_task1(self):
        while True:
            if self.flag and self.tasks1.empty():
                break
            if self.tasks1.empty():
                time.sleep(0.5)
                continue
            zd=self.tasks1.get()
            self.tasks1.task_done()
            lt1=self.snmpwalk_server(zd["ip"],"iso.3.6.1.4.1.21317.1.14.2.1.3")
            lt2=self.snmpwalk_server(zd["ip"],"iso.3.6.1.4.1.21317.1.14.2.1.4")
            lt3=self.snmpwalk_server(zd["ip"],"iso.3.6.1.4.1.21317.1.14.2.1.5")
            result=self.demo(lt1,lt2,lt3)
            zd["voltage"]=result[0];zd["current"]=result[1];zd["power"]=result[2]
            with self.lock2:
                self.result.append(zd)
            with self.lockc1:
                self.count1+=1

    def process_task1_main(self):
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for _ in range(25):
                pool.append(executor.submit(self.process_task1))
            for task in as_completed(pool):
                task.result()

    def process_task2(self):
        while True:
            if self.flag and self.tasks2.empty():
                break
            if self.tasks2.empty():
                time.sleep(0.5)
                continue
            zd=self.tasks2.get()
            self.tasks2.task_done()
            if zd["brand"]=="dell inc.":
                m=Dell(zd["ip"],self.zd2[zd["ip"]][0],self.zd2[zd["ip"]][1])
            else:
                m=Huawei(zd["ip"],self.zd2[zd["ip"]][0],self.zd2[zd["ip"]][1])
            result=m.get_psu_detail()
            lt1,lt2,lt3=result[0],result[1],result[2]
            result=self.demo(lt1,lt2,lt3)
            zd["voltage"]=result[0];zd["current"]=result[1];zd["power"]=result[2]
            with self.lock2:
                self.result.append(zd)
            with self.lockc2:
                self.count2+=1

    def process_task2_main(self):
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for _ in range(25):
                pool.append(executor.submit(self.process_task2))
            for task in as_completed(pool):
                task.result()

    def post_else(self):
        while True:
            if self.flag and self.tasks3.empty():
                break
            if self.tasks3.empty():
                time.sleep(0.5)
                continue
            zd=self.tasks3.get()
            self.tasks3.task_done()
            if zd["ip"] in self.zd2:
                cmd=f"ipmitool -I lanplus -H {zd['ip']} -U {self.zd2[zd['ip']][0]} -P '{self.zd2[zd['ip']][1]}' sensor"
                if zd["brand"]=="lenovo":
                    cmd=f"ipmitool -I lanplus -H {zd['ip']} -U {self.zd2[zd['ip']][0]} -P '{self.zd2[zd['ip']][1]}' -C 17 sensor"
                while True:
                    with self.lock1:
                        if len(self.task_pool)>=1024:
                            time.sleep(0.5)
                            continue
                        break
                try:
                    proc=subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        shell=True
                    )
                    with self.lock1:
                        self.task_pool.append((proc,zd,cmd,time.time()))
                except Exception as e:
                    logging_error.error("="*50)
                    logging_error.error("系统错误。")
                    logging_error.error(zd)
                    logging_error.error(cmd)
                    logging_error.error(e)
                    logging_error.error("="*50)
            else:
                logging_error.error("="*50)
                logging_error.error(zd)
                logging_error.error("账号以及密码缺失。")
                logging_error.error("="*50)

    def process_else_demo2(self,message):
        lt1=[];lt2=[];lt3=[]
        for line in message.split("\n"):
            line=line.strip().lower()
            try:
                if "ps" in line and "vin" in line:
                    line=line.split("|")
                    lt1.append(eval(line[1]))
                    continue
                if "ps" in line and "iin" in line:
                    line=line.split("|")
                    lt2.append(eval(line[1]))
                    continue
                if ("ps" in line and "pin" in line) or ("psu" in line and "power" in line and "in" in line) or "pw consumption" in line or "sys_power" in line or "sys power" in line or "total_power" in line:
                    line=line.split("|")
                    lt3.append(eval(line[1]))
                    continue
            except:
                pass
        return self.demo(lt1,lt2,lt3)

    def process_else_demo1(self,proc,zd,cmd,return_code):
        if return_code==0:
            stdout,stderr=proc.communicate()
            result=self.process_else_demo2(stdout)
            zd["voltage"]=result[0];zd["current"]=result[1];zd["power"]=result[2]
            with self.lock2:
                self.result.append(zd)
        elif return_code==1:
            stdout,stderr=proc.communicate()
            logging_error.error("="*50)
            logging_error.error("未知错误。")
            logging_error.error(zd)
            logging_error.error(cmd)
            logging_error.error(stderr)
            logging_error.error("="*50)
        elif return_code==-1:
            logging_error.error("="*50)
            logging_error.error("超时。")
            logging_error.error(zd)
            logging_error.error(cmd)
            logging_error.error("="*50)
        self.count3+=1

    def process_else(self):
        while True:
            with self.lock1:
                if self.flag and self.tasks3.empty() and len(self.task_pool)==0:
                    break
            completeds_info=[]
            with self.lock1:
                for idx,task in enumerate(self.task_pool):
                    proc,zd,cmd,create_time=task
                    return_code=proc.poll()
                    if return_code is not None:
                        completeds_info.append((proc,zd,cmd,return_code,idx))
                    elif (time.time()-create_time)>180:
                        completeds_info.append((proc,zd,cmd,-1,idx))
            for completed_info in completeds_info:
                proc,zd,cmd,return_code,idx=completed_info
                if return_code==-1:
                    proc.terminate();time.sleep(0.5)
                    if proc.poll() is None:
                        proc.kill()
                self.process_else_demo1(proc,zd,cmd,return_code)
            with self.lock1:
                for idx in reversed([i[-1] for i in completeds_info]):
                    del self.task_pool[idx]
            time.sleep(1)

    def snmpwalk_network(self,ip,oid):
        try:
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} {oid}"
            process=subprocess.run(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=10
            )
            temp=[]
            for line in process.stdout.strip().split("\n"):
                s=line[line.index(":")+1:].strip().strip("\"")
                temp.append(eval(s)/1000)
            return temp
        except:
            logging_error.error(command)
            return [] 

    def process_task4(self):
        while True:
            if self.flag and self.tasks4.empty():
                break
            if self.tasks4.empty():
                time.sleep(0.5)
                continue
            zd=self.tasks4.get()
            self.tasks4.task_done()
            lt1=self.snmpwalk_network(zd["ip"],"1.3.6.1.4.1.2011.5.25.31.1.1.18.1.8")
            lt2=self.snmpwalk_network(zd["ip"],"1.3.6.1.4.1.2011.5.25.31.1.1.18.1.7")
            lt3=self.snmpwalk_network(zd["ip"],"1.3.6.1.4.1.2011.6.157.1.6")
            result=self.demo(lt1,lt2,lt3)
            zd["voltage"]=result[0];zd["current"]=result[1];zd["power"]=result[2]
            with self.lock2:
                self.result.append(zd)
            with self.lockc3:
                self.count4+=1

    def process_task4_main(self):
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for _ in range(25):
                pool.append(executor.submit(self.process_task4))
            for task in as_completed(pool):
                task.result()

    def post_main(self):
        for i in self.zd1:
            for j in self.zd1[i]:
                if j[-1]=="server":
                    self.post_server(j[0],j[1],j[2],j[3],i)
                elif j[-1]=="network":
                    self.post_network(j[0],j[1],j[2],j[3],i)
        self.flag=True

    def monitor(self):
        while True:
            with self.lockc1:
                count1=self.count1
            with self.lockc2:
                count2=self.count2
            with self.lock1:
                task_pool_size=len(self.task_pool)
            count3=self.count3
            with self.lockc3:
                count4=self.count4
            task1_size=self.tasks1.qsize()
            task2_size=self.tasks2.qsize()
            task3_empty=self.tasks3.empty()
            task4_size=self.tasks4.qsize()
            logging_monitor.info(
                f"任务发送是否完成：{self.flag}，"
                f"任务1还有{task1_size}个，任务1已经完成{count1}个，"
                f"任务2还有{task2_size}个，任务2已经完成{count2}个，"
                f"其他任务发送是否完成：{self.flag and task3_empty}，"
                f"任务3还有{task_pool_size}个，任务3已经完成{count3}个，"
                f"任务4还有{task4_size}个，任务4已经完成{count4}个。"
            )
            if self.flag and task1_size == 0 and task2_size == 0 and task3_empty and task_pool_size == 0 and task4_size == 0:
                break
            time.sleep(10)

if __name__=="__main__":
    config1={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    config2={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    config3={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "clickhouse":{
            "HOST":"10.216.140.107",
            "PORT":9000,
            "USERNAME":"default",
            "PASSWORD":""
        }
    }
    m=Run(config1,config2,config3)
    s_time=time.perf_counter()
    m.run()
    e_time=time.perf_counter()
    total_cost=e_time-s_time
    logging_monitor.info(f"总耗时一共为：{total_cost:.2f} 秒。")