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

from confluent_kafka import Consumer, KafkaException, KafkaError
import sys,json,ast

from datetime import date, datetime, timedelta
import pymysql.cursors


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0
    print('assign', partitions)
    consumer_instance.assign(partitions)


if __name__ == '__main__':
    # 步驟1.設定要連線到Kafka集群的相關設定
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    props = {
        'bootstrap.servers': '10.120.26.31:9092',       # Win10 vscode 執行的位置 Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': '31',                     # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'earliest',             # Offset從最前面開始
        'session.timeout.ms': 6000,                  # consumer超過6000ms沒有與kafka連線，會被認為掛掉了
        'error_cb': error_cb                         # 設定接收error訊息的callback函數
    }


        #连接配置信息
    config = {
            'host':'127.0.0.1',
            'port':3306,
            'user':'tibame',
            'password':'Tibame1234',
            'db':'kafka_consume',
            'charset':'utf8mb4',
            'cursorclass':pymysql.cursors.DictCursor,
            }
    # 创建连接
    connection = pymysql.connect(**config)


    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = "car_test"
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName], on_assign=my_assign)
    # 步驟5. 持續的拉取Kafka有進來的訊息
    count = 0
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% {} [{}] reached end at offset {} - {}\n'.format(record.topic(),
                                                                                             record.partition(),
                                                                                             record.offset()))

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    # 秀出metadata與msgKey & msgValue訊息
                    count += 1

                    msg_dict = ast.literal_eval(msgValue)
                    print(msg_dict['power'])
                    # print(msg_dict['userId'],msg_dict['brand'],msg_dict['type'])
                    #執行sql語句
                    # try:
                    with connection.cursor() as cursor:
                        # 插入紀錄
                        # sql = 'INSERT INTO car_estimation (userId,brand,type,year,cc,power,sys,time,auto_chair) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        sql = 'INSERT INTO car_estimation (userId,brand,type,year,cc,power,sys,time,auto_chair) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)'
                        # cursor.execute(sql, ('Robin2', 'Zhyea', tomorrow, 'M', date(1988, 6, 14)));
                        if msg_dict['auto'] == 1:
                            sys = '自排'
                        elif msg_dict['hand'] == 1:
                            sys = '手排'
                        cursor.execute(sql, (msg_dict['userId'], msg_dict['brand'],msg_dict['type'],2020-int(msg_dict['year']),int(msg_dict['cc'])*1000,msg_dict['power'],sys,msg_dict['time'],msg_dict['auto_chair']));
                    # 沒有設置默認自動提交,需要自動提交,已保存所執行的語句
                    connection.commit()

                    # finally:
                    #     connection.close();

                    print('{}-{}-{} : ({} , {})'.format(topic, partition, offset, msgKey, msgValue))
                    # userId = msgValue.split(',')[3].split(':')
                    # print(userId[0])

    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(e)

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()





# from datetime import date, datetime, timedelta
# import pymysql.cursors

# #连接配置信息
# config = {
#           'host':'127.0.0.1',
#           'port':3306,
#           'user':'root',
#           'password':'zhyea.com',
#           'db':'employees',
#           'charset':'utf8mb4',
#           'cursorclass':pymysql.cursors.DictCursor,
#           }
# # 创建连接
# connection = pymysql.connect(**config)

# # 获取明天的时间
# tomorrow = datetime.now().date() + timedelta(days=1)

# # 执行sql语句
# try:
#     with connection.cursor() as cursor:
#         # 执行sql语句，插入记录
#         sql = 'INSERT INTO employees (first_name, last_name, hire_date, gender, birth_date) VALUES (%s, %s, %s, %s, %s)'
#         cursor.execute(sql, ('Robin', 'Zhyea', tomorrow, 'M', date(1989, 6, 14)));
#     # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
#     connection.commit()

# finally:
#     connection.close();













