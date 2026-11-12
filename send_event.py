import os
import json
from azure.eventhub import EventHubProducerClient, EventData

# Event Hub 연결 문자열 (환경변수에서 읽음)
conn_str = os.environ.get('EVENT_HUB_CONN_STR')
if not conn_str:
    print("❌ ERROR: EVENT_HUB_CONN_STR environment variable not set")
    exit(1)

# 메시지 준비
with open('scoring_message.json', 'r') as f:
    message_data = json.load(f)

message = json.dumps(message_data, indent=2)

print(f"📤 Sending message to Event Hub: scoring-prod-hub")
print(f"Message: {message}\n")

# Event Hub Producer 생성
producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name="scoring-prod-hub")

try:
    # 이벤트 배치 생성 및 전송
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(message))
    producer.send_batch(event_data_batch)
    print("✅ Message sent successfully to Event Hub!")
    print(f"📊 Job that will be created: {message_data['job_name']}")
finally:
    producer.close()
