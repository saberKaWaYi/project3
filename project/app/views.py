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
#         "TIMES":3,
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

import threading

@api_view(['POST'])
def power_csv_all(request):
    batch_size=100000;offset=0
    begin_time=request.data.get("begin_time");end_time=request.data.get("end_time")
    query=f'''
    SELECT * FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' ORDER BY ts ASC LIMIT {str(batch_size)} OFFSET 
    '''
    conn=Connect_Clickhouse(config)
    client=conn.client
    temp_dir=os.path.join(os.getcwd(),"temp_files")
    os.makedirs(temp_dir,exist_ok=True)
    temp_file=tempfile.NamedTemporaryFile(
        suffix='.csv',
        delete=False,
        dir=temp_dir,
        mode="w+",
        encoding="utf-8"
    )
    temp_file.close()
    with open(temp_file.name,"w",newline="",encoding="utf-8") as f:
        header_written=False
        while True:
            query_temp=query+str(offset)
            data=conn.query(query_temp)
            if data.empty:
                break
            if not header_written:
                data.to_csv(f,header=True,index=False)
                header_written=True
            else:
                data.to_csv(f,header=False,index=False)
            del data
            offset+=batch_size
    def delete_temp_file():
        try:
            if os.path.exists(temp_file.name):
                os.remove(temp_file.name)
        except Exception as e:
            logging.error(f"删除临时文件失败: {e}")
    response=FileResponse(open(temp_file.name,'rb'))
    response['Content-Type']='text/csv'
    filename=f"from_{begin_time}_to_{end_time}_{int(time.time())}.csv"
    response['Content-Disposition']=f'attachment; filename="{urllib.parse.quote(filename)}"'
    response.delete=True
    threading.Timer(300,delete_temp_file).start()
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
            zd_temp[i[-2]]["data_info"]=[{"data":[[],[]],"unit":"V","name":"V"},{"data":[[],[]],"unit":"A","name":"A"},{"data":[[],[]],"unit":"KW","name":"A"}]
        zd_temp[i[-2]]["data_info"][0]["data"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][0]["data"][1].append(round(eval(i[0]),4))
        zd_temp[i[-2]]["data_info"][1]["data"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][1]["data"][1].append(round(eval(i[1]),4))
        zd_temp[i[-2]]["data_info"][2]["data"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][2]["data"][1].append(round(eval(i[2])/1000,4))
    for i in zd_temp:
        zd_temp[i]["data_info"][0]["max"]=round(max(zd_temp[i]["data_info"][0]["data"][1]),4)
        zd_temp[i]["data_info"][0]["min"]=round(min(zd_temp[i]["data_info"][0]["data"][1]),4)
        zd_temp[i]["data_info"][1]["max"]=round(max(zd_temp[i]["data_info"][1]["data"][1]),4)
        zd_temp[i]["data_info"][1]["min"]=round(min(zd_temp[i]["data_info"][1]["data"][1]),4)
        zd_temp[i]["data_info"][2]["max"]=round(max(zd_temp[i]["data_info"][2]["data"][1]),4)
        zd_temp[i]["data_info"][2]["min"]=round(min(zd_temp[i]["data_info"][2]["data"][1]),4)
    for i in zd_temp:
        zd_temp[i]["data_info"][0]["data"][0].insert(0,"time")
        zd_temp[i]["data_info"][0]["data"][1].insert(0,"V")
        zd_temp[i]["data_info"][1]["data"][0].insert(0,"time")
        zd_temp[i]["data_info"][1]["data"][1].insert(0,"A")
        zd_temp[i]["data_info"][2]["data"][0].insert(0,"time")
        zd_temp[i]["data_info"][2]["data"][1].insert(0,"KW")
    for i in zd_temp:
        zd["data"].append(zd_temp[i])
    return Response(zd)

@api_view(['POST'])
def rack_power_list_excel(request):
    zd={};zd["code"]=200;zd["msg"]="";zd["data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts,hostname FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse(config)
    client=conn.client
    data=conn.query(query)[["voltage","current","power","ts","hostname"]].values.tolist()
    zd_temp={}
    for i in data:
        if i[-1] not in zd_temp:
            zd_temp[i[-1]]=[]
        zd_temp[i[-1]].append(i[:4])
    temp_dir=os.path.join(os.getcwd(),"temp_files")
    os.makedirs(temp_dir,exist_ok=True)
    temp_file=tempfile.NamedTemporaryFile(
        suffix='.xlsx',
        delete=False,
        dir=temp_dir
    )
    temp_file.close()
    with pd.ExcelWriter(temp_file.name) as writer:
        for i in zd_temp:
            data=pd.DataFrame(zd_temp[i],columns=["voltage","current","power","ts"])
            data.to_excel(writer,sheet_name=i,index=False)
    response=FileResponse(open(temp_file.name,'rb'))
    response['Content-Type']='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    filename=f"{rack}_power_list.xlsx"
    response['Content-Disposition']=f'attachment; filename="{urllib.parse.quote(filename)}"'
    response.delete=True
    return response

@api_view(['POST'])
def power_csv_all_more(request):
    batch_size=100000;offset=0
    begin_time=request.data.get("begin_time");end_time=request.data.get("end_time");time_grain=request.data.get("time_grain")
    query=f'''
    SELECT
        toStartOfInterval(ts, INTERVAL {time_grain} MINUTE) AS period_start,
        city,
        data_center,
        room,
        rack,
        hostname,
        ip,
        brand,
        type,
        avg(voltage) AS avg_voltage,
        avg(current) AS avg_current,
        avg(power) AS avg_power
    FROM power.power_data
    WHERE ts >= '{begin_time}' AND ts < '{end_time}'
    GROUP BY
        city,
        data_center,
        room,
        rack,
        hostname,
        ip,
        brand,
        type,
        toStartOfInterval(ts, INTERVAL {time_grain} MINUTE)
    ORDER BY period_start, hostname
    LIMIT {batch_size} OFFSET 
    '''
    print(query)
    conn=Connect_Clickhouse(config)
    client=conn.client
    temp_dir=os.path.join(os.getcwd(),"temp_files")
    os.makedirs(temp_dir,exist_ok=True)
    temp_file=tempfile.NamedTemporaryFile(
        suffix='.csv',
        delete=False,
        dir=temp_dir,
        mode="w+",
        encoding="utf-8"
    )
    temp_file.close()
    with open(temp_file.name,"w",newline="",encoding="utf-8") as f:
        header_written=False
        while True:
            query_temp=query+str(offset)
            data=conn.query(query_temp)
            if data.empty:
                break
            if not header_written:
                data.to_csv(f,header=True,index=False)
                header_written=True
            else:
                data.to_csv(f,header=False,index=False)
            del data
            offset+=batch_size
    def delete_temp_file():
        try:
            if os.path.exists(temp_file.name):
                os.remove(temp_file.name)
        except Exception as e:
            logging.error(f"删除临时文件失败: {e}")
    response=FileResponse(open(temp_file.name,'rb'))
    response['Content-Type']='text/csv'
    filename=f"from_{begin_time}_to_{end_time}_{int(time.time())}.csv"
    response['Content-Disposition']=f'attachment; filename="{urllib.parse.quote(filename)}"'
    response.delete=True
    threading.Timer(300,delete_temp_file).start()
    return response