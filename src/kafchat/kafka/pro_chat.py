from kafka import KafkaProducer
from json import dumps
import time

p = KafkaProducer(
        #TODO
)

print("채팅 프로그램 - 메세지 발신자")
print("메세지를 입력하세요. (종료시 'exit' 입력)")

while True:
    msg = input("YOU: ")
    if msg == 'exit':
        break

    data = {'message': msg, 'time': time.time()}
    #TODO 보내기

print("채팅 종료")
