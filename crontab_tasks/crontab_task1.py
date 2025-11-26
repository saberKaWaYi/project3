from connect import *
from datetime import datetime

class Run:

    def __init__(self,config1,config2,config3):
        self.config1=config1
        self.config2=config2
        self.config3=config3
        self.db_mysql=Connect_Mysql(self.config1)
        self.db_clickhouse=Connect_Clickhouse(self.config2)
        self.db_mongo=Connect_Mongodb(self.config3)
        self.time=datetime.now()
        self.zd={}
        self.result=[]

    def create_table(self):
        sql='''
        create table if not exists cds_report.power_info (
            name varchar(100),
            type varchar(100),
            std_power varchar(100),
            average_power varchar(100),
            max_power varchar(100),
            min_power varchar(100),
            average_power_rate varchar(100),
            max_power_rate varchar(100),
            min_power_rate varchar(100),
            dt date
        );
        '''
        self.db_mysql.client.cursor().execute(sql)
        self.db_mysql.client.commit()

    def create_zd(self):
        sql='''
        select * from power.temp;
        ''' 
        data=self.db_mysql.get_table_data("",sql)[["data_center","room","rack","power"]].values.tolist()
        for i in data:
            if i[0] not in self.zd:
                self.zd[i[0]]={}
            if i[1] not in self.zd[i[0]]:
                self.zd[i[0]][i[1]]={}
            if i[2] not in self.zd[i[0]][i[1]]:
                self.zd[i[0]][i[1]][i[2]]={}
            self.zd[i[0]][i[1]][i[2]]=eval(i[3])

    def create_rack(self):
        for i in self.zd:
            for j in self.zd[i]:
                for k in self.zd[i][j]:
                    city="庆阳";data_center=i;room=j;rack=k
                    sql=f'''
                    select ts,power from power.power_data where city='{city}' and data_center='{data_center}' and room='{room}' and rack='{rack}' and ts >= now() - INTERVAL 1 DAY;
                    '''
                    temp=self.db_clickhouse.query(sql)[["ts","power"]].values.tolist()
                    zd={}
                    for _ in temp:
                        if _[0] not in zd:
                            zd[_[0]]=0
                        zd[_[0]]+=eval(_[1])
                    data=list(zd.values())
                    if not data:
                        continue
                    x=max(data);y=min(data);z=sum(data)/len(data)
                    self.result.append((f"{city}|{data_center}|{room}|{rack}","rack",self.zd[i][j][k],round(z/1000,2),round(x/1000,2),round(y/1000,2),f"{round(z/self.zd[i][j][k]/10,2)}%",f"{round(x/self.zd[i][j][k]/10,2)}%",f"{round(y/self.zd[i][j][k]/10,2)}%",self.time))

    def create_room(self):
        for i in self.zd:
            for j in self.zd[i]:
                city="庆阳";data_center=i;room=j
                sql=f'''
                select ts,power from power.power_data where city='{city}' and data_center='{data_center}' and room='{room}' and ts >= now() - INTERVAL 1 DAY;
                '''
                temp=self.db_clickhouse.query(sql)[["ts","power"]].values.tolist()
                zd={}
                for _ in temp:
                    if _[0] not in zd:
                        zd[_[0]]=0
                    zd[_[0]]+=eval(_[1])
                data=list(zd.values())
                if not data:
                    continue
                x=max(data);y=min(data);z=sum(data)/len(data)
                count=0
                for k in self.zd[i][j]:
                    count+=self.zd[i][j][k]
                self.result.append((f"{city}|{data_center}|{room}","room",count,round(z/1000,2),round(x/1000,2),round(y/1000,2),f"{round(z/count/10,2)}%",f"{round(x/count/10,2)}%",f"{round(y/count/10,2)}%",self.time))
    
    def create_data_center(self):
        for i in self.zd:
            city="庆阳";data_center=i
            sql=f'''
            select ts,power from power.power_data where city='{city}' and data_center='{data_center}' and ts >= now() - INTERVAL 1 DAY;
            '''
            temp=self.db_clickhouse.query(sql)[["ts","power"]].values.tolist()
            zd={}
            for _ in temp:
                if _[0] not in zd:
                    zd[_[0]]=0
                zd[_[0]]+=eval(_[1])
            data=list(zd.values())
            if not data:
                continue
            x=max(data);y=min(data);z=sum(data)/len(data)
            count=0
            for j in self.zd[i]:
                for k in self.zd[i][j]:
                    count+=self.zd[i][j][k]
            self.result.append((f"{city}|{data_center}","data_center",count,round(z/1000,2),round(x/1000,2),round(y/1000,2),f"{round(z/count/10,2)}%",f"{round(x/count/10,2)}%",f"{round(y/count/10,2)}%",self.time))

    def insert_data(self):
        sql='''
        insert into cds_report.power_info (name,type,std_power,average_power,max_power,min_power,average_power_rate,max_power_rate,min_power_rate,dt) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.db_mysql.client.cursor().executemany(sql,self.result)
        self.db_mysql.client.commit()
    
    def run(self):
        self.create_table()
        self.create_zd()
        self.create_rack()
        self.create_room()
        self.create_data_center()
        self.insert_data()

if __name__=="__main__":
    config1={
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
    config2={
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
    config3={
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
    m=Run(config1,config2,config3)
    m.run()