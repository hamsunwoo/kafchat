from kafka import KafkaProducer
import json
import time


p = KafkaProducer(
        bootstrap_servers=['ec2-43-203-219-5.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("채팅 프로그램 - 메세지 발신자")
print("메세지를 입력하세요. (종료시 'exit' 입력)")

while True:
    msg = input("YOU: ")
    if msg == 'exit':
        break

    data = {'message': msg, 'time': time.time()}
    p.send('chat-group', value=data)

    p.flush()

print("채팅 종료")
