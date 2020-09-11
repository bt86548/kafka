#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka import Producer
import sys
import pandas as pd
import time
from datetime import datetime,timedelta


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 主程式進入點
if __name__ == '__main__':
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        # 'bootstrap.servers': 'localhost:9092',  # <-- 置換成要連接的Kafka集群
        'bootstrap.servers': '10.120.26.31:9092',  # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'car_test'
    msgCounter = 0
    try:
        while True:
            # news_df = pd.read_csv(r'C:\Users\Big data\Desktop\car_kafka_Producer\gpsData.csv', error_bad_lines=False)
            # gps_data = news_df.values.tolist()

            # for data in gps_data:
            #     gps_time = data[1]
            #     gps_time = datetime.strptime(gps_time, '%Y-%m-%dT%H:%M:%S.%fZ')
            #     tw_time = timedelta(hours=8) + gps_time
            #     lat = float(data[2])
            #     lon = float(data[3])
            #     location = [lat, lon]
            #     alt = data[4]
            #     speed = data[5]
            #     climb = data[6]
            #     track = data[7]
            #     eps = data[8]
            #     epx = data[9]
            #     epv = data[10]
            #     ept = data[11]
            #     fixtype = data[12]
                # userId,brand,type,year,cc,power,sys,time,auto_chair
                # 'Ub345ac0c415317c0fc82285ece6e06ad', 'Toyota', 'Yaris', '2017', '2000', '2', '自排', '2020-09-03 12:38', '1'
                loc_dict = {
                    # "time_stamp" : time_stamp,
                    "userId": 'Ub345ac0c415317c0fc82285ece6e06ad',
                    "brand": 'Toyota',
                    "type": 'Yaris',
                    "year": '2017',
                    "cc": '2000',
                    "power": '2',
                    "auto":'1',
                    "hand": '0',
                    "time": '2020-09-03 12:38',
                    "auto_chair": '1'
                }
                c = str(loc_dict)

                # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
                producer.produce(topicName, value=bytes(c,encoding="utf8"))

                # producer.flush()

                print('Send ' + str(msgCounter) + ' messages to Kafka' + c)
                time.sleep(1)
                msgCounter += 1
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()

