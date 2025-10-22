from connect import Connect_Mongodb
from bson import ObjectId
import pandas as pd

def get_relationship(config,city_ObjectId):
    conn=Connect_Mongodb(config)
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
    data1=conn.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"network","position_id":ObjectId(city_ObjectId)},{"position_id":1,"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["position_id","data_center_id","room_id","rack_id","device_id"]].values.tolist()
    data2=conn.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"server","position_id":ObjectId(city_ObjectId)},{"position_id":1,"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["position_id","data_center_id","room_id","rack_id","device_id"]].values.tolist()
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

def get_ObjectId(config,city_name):
    conn=Connect_Mongodb(config)
    city_ObjectId=conn.get_collection("cds_ci_att_value_position",{"status":1,"city":city_name},{"_id":1})["_id"].values.tolist()[0]
    return city_ObjectId

if __name__=="__main__":
    config={
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
    zd=get_relationship(config,get_ObjectId(config,"庆阳"))
    print(zd)