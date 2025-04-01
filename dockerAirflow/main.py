from fastapi import FastAPI
from control_scripts import Masterdata
from pathlib import Path
from json import load
from pydantic import BaseModel

class data_request(BaseModel):
    report_type_id: int
    bank_ids: int | tuple
    period_ids: int | tuple

class data_fetch(BaseModel):
    _data_request: data_request | None
    _data:  None = None
    _master_data: None = None

    def add_item_to_dict(self, d, newKey):
        d.update(newKey)
        return d
    
    def get_data(self):
        self._data = self._master_data.get_ent_attributes(self._data_request.report_type_id, 
                            self._data_request.period_ids, 
                            self._data_request.bank_ids)
        self._data = [self.add_item_to_dict(x[0],{"ent":x[1]}) for x in self._data]

    @property
    def data_request(self):
        return self._data_request

    @property
    def data(self):
        return self._data
    
    @property
    def master_data(self):
        return self._master_data

    @data_request.setter
    def data_request(self, request:data_request):
        self._data_request = request
        if type (self._data_request.period_ids) is int:
            self._data_request.period_ids = (self._data_request.period_ids, )
        elif type (self._data_request.period_ids) is tuple:
            pass
        else: raise Exception.ValueError("period_ids expects tuple of integers")

        if type (self._data_request.bank_ids) is int:
            self._data_request.bank_ids = (self._data_request.bank_ids, )
        elif type (self._data_request.bank_ids) is tuple:
            pass
        else: raise Exception.ValueError("bank_ids expects tuple of integers")

    @master_data.setter
    def master_data(self, master_data):
        self._master_data = master_data

 
app = FastAPI()

path = Path(__file__).parent
print (f'path of config file:  {path}')
conn_info = Masterdata (load (open(path / ("config/email_conf.json"))))

d_fetch =  data_fetch()
d_fetch.master_data = conn_info  

@app.post("/get_data")
def get_attributes(request:data_request):
    d_fetch.data_request = request
    d_fetch.get_data()
    return d_fetch.data

x = get_attributes(data_request(report_type_id=4, period_ids=12, bank_ids=12))
