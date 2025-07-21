from datetime import datetime, timedelta, date 
import psycopg2
from io import BytesIO
import openpyxl
from openpyxl.styles.protection import Protection
from pathlib import Path
# from  modules.my_excel_conf import excel_my_cnfg
# from  modules.conf import exportFile
from psycopg2.extras import execute_values
from datetime import datetime
import io
import pandas as pd
import json 
import traceback
import collections 

def start_parse(schedule_id):
    try:
        # path = Path(__file__).parent.parent.parent
        path = Path(__file__).parent.parent.parent
        print (f'path of config file:  {path}')
        conf = json.load (open(path / ("config/email_conf.json")))  
        db_host = conf["db_host"]
        db_port = conf["db_port"]
        db_name = conf["db_name"]
        db_user = conf["db_user"]
        db_pass = conf["db_pass"]

        connection = psycopg2.connect(user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port,
        database=db_name)
        cursor = connection.cursor()

        configs = {
             "pair_default":{
                "17559":24406,
                "15701":22602,
                "15703":22604,
                "15705":22606,
                "15501":24602,
                "15503":24604,
                "15505":24606,
                "15507":24608,
                "24406":17559,
                "22602":15701,
                "22604":15703,
                "22606":15705,
                "24602":15501,
                "24604":15503,
                "24606":15505,
                "24608":1550
        },
            "errors":{
            "0001":{
                "en":"",
                "ru":"Неверное значение в ячейке, возможно есть ссылка, формула с ошибкой",
                "tj":"",
            },
            "0002":{
                "en":"",
                "ru":"Неверный тип данных в ячейке",
                "tj":"",
            },
            "0003":{
                "en":"",
                "ru":"Ячейке не должна быть пустой",
                "tj":"",
            },
            "0004":{
                "en":"",
                "ru":"Значение не должен быть отрицательной",
                "tj":"",
            },
            "0005":{
                "en":"",
                "ru":"В ячейке указанно слишком длинное значение",
                "tj":"",
            },
            "0006":{
                "en":"",
                "ru":"Значение не соответствует требуемому словарю",
                "tj":"",
            },
            "0007":{
                "en":"",
                "ru":"В файле найденно более 1000 ошибок, выполните тчательный анализ отчета и исправьте ошибки",
                "tj":"",
            },
            "0101":{
                "en":"",
                "ru":"Неправильное правило сравнения ячеек (тип возвращаемого значения",
                "tj":"",
            },
            "0201":{
                "en":"",
                "ru":"Срок сдачи отчета истек, обратитесь к контактному лицу НБТ по данному отчету\n",
                "tj":"" 
            },
            "0202":{
                "en":"",
                "ru":"Предоставление отчета невозможно, отчетный перид еще не закрыт\n",
                "tj":"" 
            },
            "0203":{
                "en":"",
                "ru":"Название отчета указанно не правильно",
                "tj":"" 
            },
            "0204":{
                "en":"",
                "ru":"Листы и их наименование в данном отчете не соответствует стандарту \n",
                "tj":"" 
            },
            "02041":{
                "en":"",
                "ru":"в файле присуствуют лишние листы \n",
                "tj":"" 
            },
            "02042":{
                "en":"",
                "ru":"в файле не хватает обязательных листов \n",
                "tj":"" 
            },
            "0205":{
                "en":"",
                "ru":"Для получения данного отчета не существует расписание \n",
                "tj":"" 
            },
            "0206":{
                "en":"",
                "ru":"Период указан неправильно \n",
                "tj":"" 
            },
            "0207":{
                "en":"",
                "ru":"Нет данных для данного КФО на этот период \n",
                "tj":"" 
            },
        }
        }  
        
        def selectOne(table,prop,value,get_arguments=None):
                        get_arguments = "id" if get_arguments is None else get_arguments 
                        postgres_insert_query = f"select {get_arguments} from sma_stat_dep.{table} WHERE {value}" if prop is None else f"select {get_arguments} from sma_stat_dep.{table} WHERE {prop}='{value}'" 
                        cursor.execute(postgres_insert_query) 
                        try:
                            if get_arguments=="id":
                                return cursor.fetchone()[0]
                            else:
                                 return cursor.fetchone()
                        except:
                             return False   
          
        def parse_st(report_name,period,bank):
            logs = {
                    "count":0,
                    "comparisen_rules_count":0,
                    "context":'\n',
                    'upload_date':datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                }
            try:
                report = selectOne('tbl_report_type','code',report_name,'id,report_period_type,submition_mode,validation_config') 
                print(f"---report:{report}")
                bank_id = selectOne('tbl_entities','bic4',bank)
                period_id = selectOne('tbl_period',None,f"type={report[1]} and to_date="+f"'{period}'")
                if(bank_id is False):
                    raise Exception('Такой банк не существует')
                if(report is None):
                    raise Exception(configs['errors']['0203']['ru'])
                if(period_id is None):
                    raise Exception(configs['errors']['0206']['ru'])
                postgres_insert_query = f"select * from sma_stat_dep.tbl_schedule WHERE bank_id={bank_id} AND period_id={period_id} AND report_type_id={report[0]}" 
                cursor.execute(postgres_insert_query)
                schedule_records = cursor.fetchone()
                if(period_id is schedule_records):
                    raise Exception(configs['errors']['0205']['ru'])
                table = json.loads(report[3])["tables"][0] 
                # print(f"fileID--------------------------{file_id}")
                dayGet = datetime.strptime(period,'%Y-%m-%d').day
                monthGet = datetime.strptime(period,'%Y-%m-%d').month
                yearGet = datetime.strptime(period,'%Y-%m-%d').year 
                print(f"{dayGet}-{monthGet}-{yearGet}-{bank}")
                wb = pd.read_html(f'http://10.10.64.36:80/mim_file/vr_BALANCE_MIM_TXTlist.asp?x_REPORT_DATE={dayGet}%2E{monthGet}%2E{yearGet}&z_REPORT_DATE=%3D&x_BIC4={bank}&z_BIC4=%3D')
                # print(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!{wb[1]}')
                data = []
                for k in wb[0].values:
                    if(len(k[0])<100):
                         data.append(k[0].split())
                db_query_values = [] 
                if(len(data)==0):
                    # print("error!!!!!!!")
                    raise Exception(configs['errors']['0207']['ru']) 
                
                ent_id = selectOne('tbl_ent','code',table['nodes'][0]['code'])
                postgres_insert_query = f"""
                WITH inserted AS(
                INSERT INTO sma_stat_dep.tbl_file_upload 
                (uid,fetch_id,email_datetime,uploaded_datetime,email_from,upload_status,channel)	VALUES 
                ('OSB_parsing:'|| NOW(),'omor@nbt.tj_'|| NOW(),NOW(),NOW(),'mpirov@nbt.tj',4,2) RETURNING id),
                inserted_file AS (
                INSERT INTO sma_stat_dep.tbl_files
                (id_file_upload,upload_status,file_name,file)	
                SELECT id,4,'omor@nbt.tj_'|| NOW(),'' FROM inserted RETURNING id)
                INSERT INTO sma_stat_dep.tbl_file_per_schedule(file_id,schedule_id)  SELECT id,{schedule_records[0]} FROM inserted_file RETURNING id,file_id
                ;"""
                
                cursor.execute(postgres_insert_query)
                connection.commit()
                retur_file_per_schedule_id = cursor.fetchone() 
                file_per_schedule_id = retur_file_per_schedule_id[0] 
                file_id = retur_file_per_schedule_id[1] 
                for k in data: 
                    obj = {}
                    offsetting_balance = ''
                    # print(k)
                    for i in table['nodes'][0]['attribute']:
                        # configs["pair_default"]
                        if(i['attr_type']=='account_number'):
                            obj.update({i['attr_type']:k[0]}) 
                        if(i['attr_type']=='balance_dt'):
                            obj.update({i['attr_type']:k[1]}) 
                        if(i['attr_type']=='balance_ct'):
                            obj.update({i['attr_type']:k[2]})   
                        if(i['attr_type']=='offsetting_balance'):
                            offsetting_balance = i['attr_type']
                    acc_num = k[0]
                    if acc_num in configs["pair_default"]:
                        pair_number = configs["pair_default"][acc_num]
                        for ell in data:
                            if(str(pair_number)==str(ell[0])):
                                obj.update({offsetting_balance:int(ell[1])+int(ell[2])})

                    db_query_values.append([ent_id,file_per_schedule_id,json.dumps(obj,ensure_ascii=False)]) 
                execute_values(cursor,"INSERT INTO sma_stat_dep.tbl_attr_values (ent_id,file_per_schedule_id,a_value) VALUES %s",db_query_values)
                connection.commit()
                postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_schedule SET reporting_window='0' WHERE id='{schedule_records[0]}';"""
                cursor.execute(postgres_insert_query1) 
                connection.commit()
                status = 4
                log_to_text = f"Дата и время получения файла -------------------- {logs['upload_date']}\n\n {logs['context']}\n количество найденных ошибок ---------------------------{(logs['count']+logs['comparisen_rules_count'])}" 
                postgres_insert_query = f"""UPDATE sma_stat_dep.tbl_files SET logs='{log_to_text}', upload_status='{status}' WHERE id='{file_id}';"""
                cursor.execute(postgres_insert_query) 
                connection.commit()
                return True
            except (Exception, psycopg2.Error) as error: 
                # logs["context"]+=f"--{error}\n"    
                # logs["count"]+=1
                # status = 5
                print("Error: ", error,traceback.print_exc())
                return False
            finally:
                print("parsing finished")
                
        


 
        schedule_records = selectOne("tbl_schedule","id",schedule_id,"*")
        # print(f":::::::::::::::::{schedule_records}")
        perriod = str(selectOne("tbl_period","id",schedule_records[3],"to_date")[0].isoformat())
        report = selectOne("tbl_report_type","id",schedule_records[1],"code")[0]
        bank = selectOne("tbl_entities","id",schedule_records[2],"bic4")[0]
        # parse_st("OSB",'2024-04-30','5707')
        parse_res = parse_st(report,perriod,bank)
        print(f"parse result:{parse_res}")
        if(parse_res==True):
            return True
        else: return False
        

    finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
if __name__ == "__main__":    
    start_parse() 



