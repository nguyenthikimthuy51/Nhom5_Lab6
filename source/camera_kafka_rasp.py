from picamera import PiCamera
from kafka import KafkaProducer, KafkaConsumer
import base64
import time

KAFKA_SERVERS = '172.31.10.244:9092'

# Topic để gửi message đến kafka server
topic_name = "send_image"
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVERS],
    max_request_size=9000000,
)

# Topic để nhận message kafka server
topic_name_out = "receive_result"
consumer = KafkaConsumer(
    topic_name_out,
    bootstrap_servers=[KAFKA_SERVERS],
    auto_offset_reset='latest',
    enable_auto_commit=True
)

# Hàm mã hóa hình ảnh thành chuỗi Base64
def encode_image_to_base64(image_path):
    with open(image_path, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_image

# Hàm chụp ảnh từ camera raspberry
def capture_img(image_path):
    camera = PiCamera()
    camera.start_preview()
    time.sleep(2)  # Sleep to allow the camera to adjust to light conditions
    camera.capture(image_path)
    camera.stop_preview()

image_path = 'image.jpg' 
capture_img(image_path)
encoded_image_data = encode_image_to_base64(image_path)

# Gửi ảnh đã được mã hóa đến Kafka Server
producer.send(topic_name, encoded_image_data.encode('utf-8'))
producer.flush()
print("Image sent to Kafka topic: {}".format(topic_name))

#Lắng nghe topic receive_result để nhận message từ kafka server
try:
    for result_msg in consumer:
        result_data = result_msg.value.decode('utf-8')
        print("Received result from Kafka topic {}: {}".format(topic_name_out, result_data))
except Exception as e:
    print("Error consuming messages from Kafka topic {}: {}".format(topic_name_out, e))

producer.close()
