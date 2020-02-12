from influxdb import InfluxDBClient
from datetime import date,timedelta,datetime
import socket
import json

client = InfluxDBClient('manager01.data.lake', 8086, 'big_data_monitoring', '', 'big_data_monitoring')
now_dt = datetime.now() - timedelta(hours=1)
sys_hostname = socket.gethostname()


def build_gpustat_user_json(time, measurement, hostname, user, value):
    json_body = [{'fields': {'value': value},
                  'measurement': measurement,
                  'tags': {'hostname': hostname, 'user': user},
                  'time': time}]
    return json_body


def build_gpustat_device_json(time, measurement, hostname, user, value):
    json_body = [{'fields': {'value': value},
                  'measurement': measurement,
                  'tags': {'hostname': hostname, 'device': user},
                  'time': time}]
    return json_body


def gpustat_list_user(data):
    lst = []
    for i in range(len(data['gpus'])):
        for j in range(len(data['gpus'][i]['processes'])):
            lst.append(build_gpustat_user_json(now_dt, #time
                                               f"user.gpu.{data['gpus'][i]['name']}".lower().replace(' ','_'), #measurement
                                               sys_hostname, #hostname
                                               data['gpus'][i]['processes'][j]['username'], #user
                                               data['gpus'][i]['processes'][j]['gpu_memory_usage'])) #value

    return lst


def gpustat_list_device(data):
    lst = []
    for i in range(len(data['gpus'])):
        lst.append(build_gpustat_device_json(now_dt, #time
                                             "device.gpu", #measurement
                                             sys_hostname, #hostname
                                             f"{data['gpus'][i]['name']}".lower().replace(' ','_'), #device
                                             data['gpus'][i]['memory.used'])) #value

    return lst


with open("gpustat_json.json", 'r') as json_file:
    json_data = json.load(json_file)

for i in gpustat_list_user(json_data):
    print(i)
    #client.write_points(i)

for i in gpustat_list_device(json_data):
    print(i)
    #client.write_points(i)
