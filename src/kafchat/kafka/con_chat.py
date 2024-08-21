from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        'chat',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id="chat-group",
        enable_auto_commit=True,
)

print("채팅 프로그램 - 메세지 수신")
print("메세지 대기중...")

try:
    for m in consumer:
        data = m.value
        print(f"[FRIEND] {data['message']}")

except KeyboardInterrupt:
    print("채팅 종료")
finally:
    consumer.close()
