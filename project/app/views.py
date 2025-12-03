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
import time
import pandas as pd

from clickhouse_driver import Client

class Connect_Clickhouse:

    def __init__(self):
        self.config={
            "connection":{
                "TIMES":3,
                "TIME":1
            },
            "clickhouse":{
                "HOST":"10.216.140.107",
                "PORT":9000,
                "USERNAME":"default",
                "PASSWORD":""
            }
        }
        # self.config={
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
    
from pymongo import MongoClient
    
class Connect_Mongodb:

    def __init__(self):
        self.config={
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
        # self.config={
        #     "connection":{
        #         "TIMES":1000,
        #         "TIME":0.1
        #     },
        #     "mongodb":{
        #         "HOST":"localhost",
        #         "PORT":4000,
        #         "USERNAME":"manager",
        #         "PASSWORD":"cds-cloud@2017"
        #     }
        # }
        self.client=self.login()
        self.db=self.get_database()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=MongoClient(host=self.config["mongodb"]["HOST"],port=self.config["mongodb"]["PORT"])
                client.cds_cmdb.authenticate(self.config["mongodb"]["USERNAME"],self.config["mongodb"]["PASSWORD"])
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mongodb登录失败。")
        raise Exception("mongodb登录失败。")
    
    def get_database(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                return self.client.get_database("cds_cmdb")
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("cds_cmdb获取失败。")
        raise Exception("cds_cmdb获取失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.close()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mongodb关闭失败。")
        raise Exception("mongodb关闭失败。")

    def get_collection(self,name,condition1,condition2):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data=pd.DataFrame(self.db.get_collection(name).find(condition1,condition2)).astype(str)
                return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error(f"{name}数据获取失败。")
        raise Exception(f"{name}数据获取失败。")

from bson import ObjectId

def get_relationship(city_ObjectIds):
    conn=Connect_Mongodb()
    pipeline=[
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
                "hostname":1
            }
        }
    ]
    jh1=set(pd.DataFrame(list(conn.db.cds_ci_att_value_network.aggregate(pipeline))).astype(str)["hostname"].values.tolist())
    pipeline[2]["$match"]['device_server_group']={'$ne':ObjectId("5ec8c70a94285cfd9cacee9b")}
    pipeline[2]["$match"]['device_server_type']={'$ne':ObjectId("5ec8c70a94285cfd9caceebf")}
    jh2=set(pd.DataFrame(list(conn.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str)["hostname"].values.tolist())
    data1=conn.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"network","position_id":{"$in":city_ObjectIds}},{"position_id":1,"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["position_id","data_center_id","room_id","rack_id","device_id"]].values.tolist()
    data2=conn.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"server","position_id":{"$in":city_ObjectIds}},{"position_id":1,"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["position_id","data_center_id","room_id","rack_id","device_id"]].values.tolist()
    city=conn.get_collection("cds_ci_att_value_position",{"status":1},{"_id":1,"city":1})[["_id","city"]].values.tolist()
    city_dict=dict(zip([i[0] for i in city],[i[1] for i in city]))
    data_center=conn.get_collection("cds_ci_att_value_data_center",{"status":1},{"_id":1,"data_center_name":1})[["_id","data_center_name"]].values.tolist()
    data_center_dict=dict(zip([i[0] for i in data_center],[i[1] for i in data_center]))
    room=conn.get_collection("cds_ci_att_value_room",{"status":1},{"_id":1,"room_name":1})[["_id","room_name"]].values.tolist()
    room_dict=dict(zip([i[0] for i in room],[i[1] for i in room]))
    rack=conn.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"rack_name":1})[["_id","rack_name"]].values.tolist()
    rack_dict=dict(zip([i[0] for i in rack],[i[1] for i in rack]))
    network=conn.get_collection("cds_ci_att_value_network",{"status":1},{"_id":1,"hostname":1,"device_ip":1,"brand":1})[["_id","hostname","device_ip","brand"]].values.tolist()
    network_dict=dict(zip([i[0] for i in network],[(i[1],i[2],i[3],"network") for i in network]))
    server=conn.get_collection("cds_ci_att_value_server",{"status":1},{"_id":1,"hostname":1,"out_band_ip":1,"web_brand":1})[["_id","hostname","out_band_ip","web_brand"]].values.tolist()
    server_dict=dict(zip([i[0] for i in server],[(i[1],i[2],i[3],"server") for i in server]))
    data=[]
    for i in data1:
        temp=[city_dict.get(i[0],None),data_center_dict.get(i[1],None),room_dict.get(i[2],None),rack_dict.get(i[3],None),network_dict.get(i[4],None)]
        if None in temp:
            continue
        if temp[-1][0] not in jh1:
            continue
        data.append(temp)
    for i in data2:
        temp=[city_dict.get(i[0],None),data_center_dict.get(i[1],None),room_dict.get(i[2],None),rack_dict.get(i[3],None),server_dict.get(i[4],None)]
        if None in temp:
            continue
        if temp[-1][0] not in jh2:
            continue
        data.append(temp)
    zd={}
    for i in data:
        s="|".join(i[:-1])
        if s not in zd:
            zd[s]=[]
        zd[s].append(i[-1])
    temp=[]
    for i in zd:
        if not zd[i]:
            temp.append(i)
    for i in temp:
        del zd[i]
    return zd

def get_ObjectId(city_names):
    conn=Connect_Mongodb()
    city_ObjectId=conn.get_collection("cds_ci_att_value_position",{"status":1,"city":{"$in":city_names}},{"_id":1})["_id"].values.tolist()
    return [ObjectId(i) for i in city_ObjectId]

from rest_framework.decorators import api_view
from rest_framework.response import Response

@api_view(['GET'])
def menu_data(request):
    zd={}
    zd["code"]=200;zd["msg"]=""
    data=[]
    temp=get_relationship(get_ObjectId(["庆阳","达拉斯","台北"]))
    for _ in list(temp.keys()):
        _=_.split("|")
        data.append(_)
    temp={}
    for i in data:
        a,b,c,d=i[0],i[1],i[2],i[3]
        s=(a,b,c)
        if s not in temp:
            temp[s]=set()
        temp[s].add(d)
    zd["data"]=[]
    for i in temp:
        lt_temp=list(temp[i])
        lt_temp.sort()
        if i.split("-")[0]=="庆阳" and "-" in lt_temp[0]:
            lt_temp=[(int(i.split("-")[0]),int(i.split("-")[1])) for i in lt_temp]
            lt_temp.sort()
            lt_temp=[f"{i[0]}-{i[1]}" for i in lt_temp]
        zd_temp={}
        zd_temp["code"]="-".join(list(i))
        zd_temp["name"]="-".join(list(i))
        zd_temp["city"]=i[0]
        zd_temp["data_center"]=i[1]
        zd_temp["room"]=i[2]
        zd_temp["rack_list"]=lt_temp
        zd["data"].append(zd_temp)
    zd["data"]=sorted(zd["data"],key=lambda item:item["code"])
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
    zd={}
    zd["code"]=200;zd["msg"]="";zd["data"]={}
    zd["data"]["power_data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse()
    data=conn.query(query)[["voltage","current","power","ts"]].values.tolist()
    if not data:
        lt1={"max":0,"min":0,"name":"V","unit":"V","data":[["time",""],["V",""]]}
        lt2={"max":0,"min":0,"name":"A","unit":"A","data":[["time",""],["A",""]]}
        lt3={"max":0,"min":0,"name":"KW","unit":"KW","data":[["time",""],["KW",""]]}
        zd["data"]["power_data"]=[lt1,lt2,lt3]
        return Response(zd)
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
    power=[round(i/1000,2) for i in data["power"].values.tolist()]
    temp["max"]=max(power);temp["min"]=min(power);temp["name"]="KW";temp["unit"]="KW"
    lt1=data["ts"].values.tolist();lt1.insert(0,"time")
    lt2=power;lt2.insert(0,"KW")
    temp["data"]=[lt1,lt2]
    zd["data"]["power_data"].append(temp)
    conn=Connect_Mongodb()
    x=conn.get_collection("cds_ci_att_value_rack",{"status":1,"data_center_name":data_center,"room_name":room,"rack_name":rack},{"std_quantity":1})["std_quantity"].values.tolist()[0]
    zd["data"]["power_unit"]="KW"
    zd["data"]["std_quantity"]=x
    return Response(zd)

import tempfile
from django.http import FileResponse

@api_view(['POST'])
def rack_power_excel(request):
    zd={}
    zd["code"]=200;zd["msg"]="";zd["data"]={}
    zd["data"]["power_data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse()
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
        dir=temp_dir
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
    conn=Connect_Clickhouse()
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
    zd={}
    zd["code"]=200;zd["msg"]="";zd["data"]=[]
    begin_time=request.data["begin_time"];end_time=request.data["end_time"]
    city=request.data["city"];data_center=request.data["data_center"];room=request.data["room"];rack=request.data["rack"]
    query=f'''
    SELECT voltage,current,power,ts,hostname,type,ip FROM power.power_data WHERE ts >='{begin_time}' AND ts<='{end_time}' AND city='{city}' AND data_center='{data_center}' AND room='{room}' AND rack='{rack}' ORDER BY ts ASC
    '''
    conn=Connect_Clickhouse()
    data1=conn.query(query)[["voltage","current","power","ts","ip","hostname","type"]].values.tolist()
    data2=get_relationship(get_ObjectId([city]))[f"{city}|{data_center}|{room}|{rack}"]
    zd_temp={}
    for i in data1:
        if i[-2] not in zd_temp:
            zd_temp[i[-2]]={}
            zd_temp[i[-2]]["hostname"]=i[-2]+"("+i[-3]+")"
            zd_temp[i[-2]]["type"]=i[-1]
            zd_temp[i[-2]]["data_info"]=[{"data":[[],[]],"unit":"V","name":"V"},{"data":[[],[]],"unit":"A","name":"A"},{"data":[[],[]],"unit":"KW","name":"KW"}]
        zd_temp[i[-2]]["data_info"][0]["data"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][0]["data"][1].append(round(eval(i[0]),2))
        zd_temp[i[-2]]["data_info"][1]["data"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][1]["data"][1].append(round(eval(i[1]),2))
        zd_temp[i[-2]]["data_info"][2]["data"][0].append(i[3])
        zd_temp[i[-2]]["data_info"][2]["data"][1].append(round(eval(i[2])/1000,2))
    for i in zd_temp:
        zd_temp[i]["data_info"][0]["max"]=round(max(zd_temp[i]["data_info"][0]["data"][1]),2)
        zd_temp[i]["data_info"][0]["min"]=round(min(zd_temp[i]["data_info"][0]["data"][1]),2)
        zd_temp[i]["data_info"][1]["max"]=round(max(zd_temp[i]["data_info"][1]["data"][1]),2)
        zd_temp[i]["data_info"][1]["min"]=round(min(zd_temp[i]["data_info"][1]["data"][1]),2)
        zd_temp[i]["data_info"][2]["max"]=round(max(zd_temp[i]["data_info"][2]["data"][1]),2)
        zd_temp[i]["data_info"][2]["min"]=round(min(zd_temp[i]["data_info"][2]["data"][1]),2)
    for i in zd_temp:
        zd_temp[i]["data_info"][0]["data"][0].insert(0,"time")
        zd_temp[i]["data_info"][0]["data"][1].insert(0,"V")
        zd_temp[i]["data_info"][1]["data"][0].insert(0,"time")
        zd_temp[i]["data_info"][1]["data"][1].insert(0,"A")
        zd_temp[i]["data_info"][2]["data"][0].insert(0,"time")
        zd_temp[i]["data_info"][2]["data"][1].insert(0,"KW")
    for i in zd_temp:
        zd["data"].append(zd_temp[i])
    jh=set()
    for i in zd["data"]:
        jh.add(i["hostname"])
    for i in data2:
        hostname=i[0];ip=i[1];brand=i[2];type_=i[3]
        hostname="-".join([i.strip() for i in hostname.split("-")]);ip=".".join([i.strip() for i in ip.split(".")]);brand=brand.lower()
        hostname=f"{hostname}({ip})"
        if hostname in jh:
            continue
        temp={}
        temp["hostname"]=hostname;temp["type"]=type_;temp["data_info"]=[]
        lt1={"max":0,"min":0,"name":"V","unit":"V","data":[["time",""],["V",""]]}
        lt2={"max":0,"min":0,"name":"A","unit":"A","data":[["time",""],["A",""]]}
        lt3={"max":0,"min":0,"name":"KW","unit":"KW","data":[["time",""],["KW",""]]}
        temp["data_info"]=[lt1,lt2,lt3]
        zd["data"].append(temp)
    return Response(zd)

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
    conn=Connect_Clickhouse()
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