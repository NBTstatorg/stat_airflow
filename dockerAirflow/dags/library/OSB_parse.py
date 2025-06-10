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

def start_parse():
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
            try:
                report = selectOne('tbl_report_type','code',report_name,'id,report_period_type,submition_mode,validation_config') 
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
                postgres_insert_query = f"""
                WITH inserted AS(
                INSERT INTO sma_stat_dep.tbl_file_upload 
                (uid,fetch_id,email_datetime,uploaded_datetime,email_from,upload_status,channel)	VALUES 
                ('OSB_parsing:'|| NOW(),'omor@nbt.tj_'|| NOW(),NOW(),NOW(),'mpirov@nbt.tj',4,2) RETURNING id),
                inserted_file AS (
                INSERT INTO sma_stat_dep.tbl_files
                (id_file_upload,upload_status,file_name,file)	
                SELECT id,4,'omor@nbt.tj_'|| NOW(),'' FROM inserted RETURNING id)
                INSERT INTO sma_stat_dep.tbl_file_per_schedule(file_id,schedule_id)  SELECT id,{schedule_records[0]} FROM inserted_file RETURNING id
                ;"""
                cursor.execute(postgres_insert_query)
                connection.commit()
                file_per_schedule_id = cursor.fetchone()[0] 
                dayGet = datetime.strptime(period,'%Y-%m-%d').day
                monthGet = datetime.strptime(period,'%Y-%m-%d').month
                yearGet = datetime.strptime(period,'%Y-%m-%d').year 
                print()
                wb = pd.read_html(f'http://10.10.64.36:80/mim_file/vr_BALANCE_MIM_TXTlist.asp?x_REPORT_DATE={dayGet}%2E{monthGet}%2E{yearGet}&z_REPORT_DATE=%3D&x_BIC4={bank}&z_BIC4=%3D')
                # print(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!{wb}')

                data = []
                for k in wb[0].values:
                    if(len(k[0])<100):
                         data.append(k[0].split()) 
                db_query_values = [] 
                ent_id = selectOne('tbl_ent','code',table['nodes'][0]['code'])

                for k in data: 
                    obj = {}
                    # print(k)
                    for i in table['nodes'][0]['attribute']:
                        if(i['attr_type']=='account_number'):
                            obj.update({i['attr_type']:k[0]}) 
                        if(i['attr_type']=='balance_dt'):
                            obj.update({i['attr_type']:k[1]}) 
                        if(i['attr_type']=='balance_ct'):
                            obj.update({i['attr_type']:k[2]})  
                    db_query_values.append([ent_id,file_per_schedule_id,json.dumps(obj,ensure_ascii=False)]) 
                execute_values(cursor,"INSERT INTO sma_stat_dep.tbl_attr_values (ent_id,file_per_schedule_id,a_value) VALUES %s",db_query_values)
                connection.commit()
                postgres_insert_query1 = f"""UPDATE sma_stat_dep.tbl_schedule SET reporting_window='0' WHERE id='{schedule_records[0]}';"""
                cursor.execute(postgres_insert_query1) 
                connection.commit()
            except (Exception, psycopg2.Error) as error: 
                print("Error while fetching data from PostgreSQL", error,traceback.print_exc())
                return False
            finally:
                print("parsing has finished complite")
                return True
        



        # trigger of starting of parse 
        path = Path(__file__).resolve().parent
        print (f'path of config file:  {path}')
        filename = path/"words.json"
        data_str = datetime.today().strftime('%d %B %Y')
        try:
            with filename.open('r', encoding='utf-8') as f:
                json_dict = json.load(f)
            plaseholders = ', '.join(['%s']*len(json_dict["data"]))
            if(data_str==json_dict["period"]):
                 print(json_dict)
            else:
                raise Exception('новый отчетный день')
            postgres_insert_query = f"select sma_stat_dep.tbl_schedule.id,sma_stat_dep.tbl_report_type.code,sma_stat_dep.tbl_entities.bic4,sma_stat_dep.tbl_period.to_date from sma_stat_dep.tbl_schedule join sma_stat_dep.tbl_report_type on sma_stat_dep.tbl_report_type.id=report_type_id join sma_stat_dep.tbl_entities on sma_stat_dep.tbl_entities.id=bank_id join sma_stat_dep.tbl_period on sma_stat_dep.tbl_period.id=period_id where reporting_window>0 and sma_stat_dep.tbl_report_type.code='OSB' and sma_stat_dep.tbl_schedule.id not in ({plaseholders}) limit 1;"
            if not plaseholders:
                raise Exception('Список исключений пуст')
        except:
            json_dict = {
                "period":data_str,
                "data":[]
            }
            with filename.open('w', encoding='utf-8') as f:
                json.dump(json_dict,f,ensure_ascii=False,indent=4)
            postgres_insert_query = f"select sma_stat_dep.tbl_schedule.id,sma_stat_dep.tbl_report_type.code,sma_stat_dep.tbl_entities.bic4,sma_stat_dep.tbl_period.to_date from sma_stat_dep.tbl_schedule join sma_stat_dep.tbl_report_type on sma_stat_dep.tbl_report_type.id=report_type_id join sma_stat_dep.tbl_entities on sma_stat_dep.tbl_entities.id=bank_id join sma_stat_dep.tbl_period on sma_stat_dep.tbl_period.id=period_id where reporting_window>0 and sma_stat_dep.tbl_report_type.code='OSB' limit 1;" 
            
        finally:
            print(postgres_insert_query)
            cursor.execute(postgres_insert_query,json_dict["data"])
            schedule_records = cursor.fetchone()
            print(schedule_records)
            datestring  = str(schedule_records[3].isoformat())
            # parse_st("OSB",'2024-04-30','5707')
            parse_res = parse_st(schedule_records[1],datestring,schedule_records[2])
            if(parse_res==False):
                json_dict["data"].append(schedule_records[0])
                with filename.open('w', encoding='utf-8') as f:
                    json.dump(json_dict,f,ensure_ascii=False,indent=4)
                print(f"file added! {filename}")

        # postgres_insert_query = f"select sma_stat_dep.tbl_schedule.id,sma_stat_dep.tbl_report_type.code,sma_stat_dep.tbl_entities.bic4,sma_stat_dep.tbl_period.to_date from sma_stat_dep.tbl_schedule join sma_stat_dep.tbl_report_type on sma_stat_dep.tbl_report_type.id=report_type_id join sma_stat_dep.tbl_entities on sma_stat_dep.tbl_entities.id=bank_id join sma_stat_dep.tbl_period on sma_stat_dep.tbl_period.id=period_id where reporting_window>0 and sma_stat_dep.tbl_report_type.code='OSB' limit 1;" 
        # cursor.execute(postgres_insert_query)
        # schedule_records = cursor.fetchone()
        # print(schedule_records)
        # datestring  = str(schedule_records[3].isoformat())
        # # parse_st("OSB",'2024-04-30','5707')
        # parse_res = parse_st(schedule_records[1],datestring,schedule_records[2])
        # if(parse_res==False):
            
        #     data.append(schedule_records[0])

        #     with filename.open('w', encoding='utf-8') as f:
        #         json.dump(data,f,ensure_ascii=False,indent=4)
        #     print(f"file added! {filename}")

    finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
if __name__ == "__main__":    
    start_parse() 



