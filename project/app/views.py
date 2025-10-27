import os
import logging
from logging.handlers import RotatingFileHandler

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir,"app.log"),
            maxBytes=1024*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)

import atexit
from clickhouse_driver import Client
import time
import pandas as pd

class Connect_Clickhouse:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=Client(host=self.config["clickhouse"]["HOST"],port=self.config["clickhouse"]["PORT"],user=self.config["clickhouse"]["USERNAME"],password=self.config["clickhouse"]["PASSWORD"])
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("clickhouse登录失败。")
        raise Exception("clickhouse登录失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.disconnect()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("clickhouse关闭失败。")
        raise Exception("clickhouse关闭失败。")
    
    def query(self,query):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data,columns=self.client.execute(query,with_column_types=True)
                columns=[col[0] for col in columns]
                data=pd.DataFrame(data,columns=columns).astype(str)
                return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error(f"{query}数据获取失败。")
        raise Exception(f"{query}数据获取失败。")

config={
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
# config={
#     "connection":{
#         "TIMES":1000,
#         "TIME":0.1
#     },
#     "clickhouse":{
#         "HOST":"localhost",
#         "PORT":5001,
#         "USERNAME":"default",
#         "PASSWORD":""
#     }
# }

from rest_framework.decorators import api_view
from rest_framework.response import Response

@api_view(['POST'])
def get_data(request):
    return Response(request.data)

from datetime import datetime,timedelta

@api_view(['GET'])
def menu_data(request):
    zd={}
    zd["code"]=200;zd["msg"]=""
    conn=Connect_Clickhouse(config)
    client=conn.client
    end_time=datetime.now()+timedelta(days=1)
    start_time=end_time-timedelta(days=2)
    start_str=start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str=end_time.strftime('%Y-%m-%d %H:%M:%S')
    query=f'''
    SELECT city,data_center,room,rack FROM power.power_data WHERE ts >='{start_str}' AND ts<='{end_str}'
    '''
    data=conn.query(query).values.tolist()
    temp={}
    for i in data:
        a,b,c,d=i[0],i[1],i[2],i[3]
        s=a+"-"+b+"-"+c
        if s not in temp:
            temp[s]=set()
        temp[s].add(d)
    zd["data"]=[]
    for i in temp:
        zd_temp={}
        zd_temp["code"]=i
        zd_temp["name"]=i
        zd_temp["rack_list"]=sorted(list(temp[i]))
        zd["data"].append(zd_temp)
    return Response(zd)

def demo(lt):
    c=sum([i[2] for i in lt])
    if c==0:
        return [0.00,0.00,0.00]
    b=sum([i[1] for i in lt])
    a=sum([i[0]*i[2] for i in lt])/c
    return [round(a,2),round(b,2),round(c,2)]

import urllib

@api_view(['POST'])
def rack_power(request):
    zd={};zd["code"]=200;zd["msg"]="";zd["data"]={}
    zd["data"]["power_data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse(config)
    client=conn.client
    data=conn.query(query)[["voltage","current","power","ts"]].values.tolist()
    temp={}
    for i in data:
        if i[3] not in temp:
            temp[i[3]]=[]
        temp[i[3]].append([eval(i[0]),eval(i[1]),eval(i[2])])
    for i in temp:
        temp[i]=demo(temp[i])
    data=[]
    for i in temp:
        data.append([temp[i][0],temp[i][1],temp[i][2],i])
    data=pd.DataFrame(data,columns=["voltage","current","power","ts"])
    temp={}
    voltage=data["voltage"].values.tolist()
    temp["max"]=max(voltage);temp["min"]=min(voltage);temp["name"]="V";temp["unit"]="V"
    lt1=data["ts"].values.tolist();lt1.insert(0,"time")
    lt2=voltage;lt2.insert(0,"V")
    temp["data"]=[lt1,lt2]
    zd["data"]["power_data"].append(temp)
    temp={}
    current=data["current"].values.tolist()
    temp["max"]=max(current);temp["min"]=min(current);temp["name"]="A";temp["unit"]="A"
    lt1=data["ts"].values.tolist();lt1.insert(0,"time")
    lt2=current;lt2.insert(0,"A")
    temp["data"]=[lt1,lt2]
    zd["data"]["power_data"].append(temp)
    temp={}
    power=[i/1000 for i in data["power"].values.tolist()]
    temp["max"]=max(power);temp["min"]=min(power);temp["name"]="KW";temp["unit"]="KW"
    lt1=data["ts"].values.tolist();lt1.insert(0,"time")
    lt2=power;lt2.insert(0,"KW")
    temp["data"]=[lt1,lt2]
    zd["data"]["power_data"].append(temp)
    return Response(zd)

import tempfile
from django.http import FileResponse

@api_view(['POST'])
def rack_power_excel(request):
    zd={};zd["code"]=200;zd["msg"]="";zd["data"]={}
    zd["data"]["power_data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse(config)
    client=conn.client
    data=conn.query(query)[["voltage","current","power","ts"]].values.tolist()
    temp={}
    for i in data:
        if i[3] not in temp:
            temp[i[3]]=[]
        temp[i[3]].append([eval(i[0]),eval(i[1]),eval(i[2])])
    for i in temp:
        temp[i]=demo(temp[i])
    data=[]
    for i in temp:
        data.append([temp[i][0],temp[i][1],temp[i][2],i])
    data=pd.DataFrame(data,columns=["voltage(V)","current(A)","power(KW)","ts"])
    temp_dir=os.path.join(os.getcwd(),"temp_files")
    os.makedirs(temp_dir,exist_ok=True)
    temp_file=tempfile.NamedTemporaryFile(
        suffix='.xlsx',
        delete=False,
        dir=os.path.join(os.getcwd(),"temp_files")
    )
    temp_file.close()
    with pd.ExcelWriter(temp_file.name) as writer:
        data.to_excel(writer,index=False)
    response=FileResponse(open(temp_file.name,'rb'))
    response['Content-Type']='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    filename=f"{city}_{data_center}_{room}_{rack}_from_{begin_time}_to_{end_time}_{int(time.time())}"
    response['Content-Disposition']=f'attachment; filename="{urllib.parse.quote(filename)}"'
    response.delete=True
    return response

@api_view(['POST'])
def rack_power_excel_all(request):
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    query=f'''
    SELECT * FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse(config)
    client=conn.client
    data=conn.query(query)
    temp_dir=os.path.join(os.getcwd(),"temp_files")
    os.makedirs(temp_dir,exist_ok=True)
    temp_file=tempfile.NamedTemporaryFile(
        suffix='.xlsx',
        delete=False,
        dir=os.path.join(os.getcwd(),"temp_files")
    )
    temp_file.close()
    with pd.ExcelWriter(temp_file.name) as writer:
        data.to_excel(writer,index=False)
    response=FileResponse(open(temp_file.name,'rb'))
    response['Content-Type']='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    filename=f"from_{begin_time}_to_{end_time}_{int(time.time())}"
    response['Content-Disposition']=f'attachment; filename="{urllib.parse.quote(filename)}"'
    response.delete=True
    return response

@api_view(['POST'])
def rack_power_list(request):
    zd={};zd["code"]=200;zd["msg"]="";zd["data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts,hostname,type FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse(config)
    client=conn.client
    data=conn.query(query)[["voltage","current","power","ts","hostname","type"]].values.tolist()
    zd_temp={}
    for i in data:
        if i[-2] not in zd_temp:
            zd_temp[i[-2]]={}
            zd_temp[i[-2]]["hostname"]=i[-2]
            zd_temp[i[-2]]["type"]=i[-1]
            zd_temp[i[-2]]["data_info"]=[{"data_value":[[],[]],"unit":"V"},{"data_value":[[],[]],"unit":"A"},{"data_value":[[],[]],"unit":"KW"}]
        zd_temp[i[-2]]["data_info"][0]["data_value"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][0]["data_value"][1].append(i[0])
        zd_temp[i[-2]]["data_info"][1]["data_value"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][1]["data_value"][1].append(i[1])
        zd_temp[i[-2]]["data_info"][2]["data_value"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][2]["data_value"][1].append(round(i[2]/1000,4))
    for i in zd_temp:
        zd_temp[i]["data_info"][0]["max"]=max(zd_temp[i]["data_info"][0]["data_value"][1])
        zd_temp[i]["data_info"][0]["min"]=min(zd_temp[i]["data_info"][0]["data_value"][1])
        zd_temp[i]["data_info"][1]["max"]=max(zd_temp[i]["data_info"][1]["data_value"][1])
        zd_temp[i]["data_info"][1]["min"]=min(zd_temp[i]["data_info"][1]["data_value"][1])
        zd_temp[i]["data_info"][2]["max"]=max(zd_temp[i]["data_info"][2]["data_value"][1])
        zd_temp[i]["data_info"][2]["min"]=min(zd_temp[i]["data_info"][2]["data_value"][1])
    for i in zd:
        zd["data"].append(zd[i])
    return Response(zd)