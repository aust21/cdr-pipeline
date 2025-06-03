import json
import random
import uuid
from datetime import timedelta
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
from faker.providers import BaseProvider
import logging

log = logging.getLogger(__name__)

status = ["outgoing", "incoming"]
call_type = ["voice", "video"]

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
REPLICATION_FACTOR = 3
NUM_PARTITIONS= 2


producer_config = {
    "bootstrap.servers": KAFKA_BROKERS,
    "queue.buffering.max.messages": 10000,
    "queue.buffering.max.kbytes": 512000,
    "batch.num.messages": 1000,
    "linger.ms": 10,
    "acks": 1,
    "compression.type": "gzip",
    "socket.timeout.ms": 60000,
    "delivery.timeout.ms": 120000,
    "debug": "broker,topic,msg" 
}

producer = Producer(producer_config)

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers":KAFKA_BROKERS})
    try:
        metadata = admin_client.list_topics(timeout=20)

        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )

            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    log.info(f"Topic created")
                    
                except Exception as e:
                    log.error("Failed to create topic")
        else:
            log.warning("Topic already exists")
    except Exception as e:
        log.error(f"An error has occured: {e}")


class ZuluStateProvider(BaseProvider):
    def state(self):
        return self.random_element([
            "KwaZulu-Natal", "Gauteng", "Western Cape", "Eastern Cape",
            "Limpopo", "Mpumalanga", "North West", "Free State", "Northern Cape"
        ])

fake = Faker("zu_ZA")
fake.add_provider(ZuluStateProvider)

def generate_phone_number():
    return fake.numerify("+27## ### ####")

def generate_cell_tower():
    region_prefixes = ["JHB", "CPT", "DBN", "PLK", "BLO", "KZN", "EC", "WC", "PTA", "NC", "FS", "NW", "MP", "LIM"]
    zone_suffixes = ["CT", "NW", "SO", "NO", "TWR"]
    region = fake.random_element(region_prefixes)
    zone = fake.random_element(zone_suffixes)
    number = fake.numerify("###")
    return f"{region}-{zone}-{number}"

def generate_call_times():
    # Generate a random start time within the past 30 days
    start_time = fake.date_time_between(start_date="-30d", end_date="now")

    # Random call duration in seconds (5 seconds to 1 hour)
    duration_seconds = fake.random_int(min=5, max=3600)

    # Calculate end time
    end_time = start_time + timedelta(seconds=duration_seconds)

    # Return as ISO-formatted strings and duration in seconds
    return {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration": duration_seconds
    }

def generate_data():
    name = fake.name()
    call_times = generate_call_times()
    receiver = fake.numerify("+27## ### ####")
    type = call_type[random.randint(0, 1)]
    return dict(
        name = name,
        user_id= str(uuid.uuid4()),
        receiver = receiver,
        call_type = type,
        start_time = call_times["start_time"],
        end_time = call_times["end_time"],
        duration = call_times["duration"],
        cell_tower = generate_cell_tower(),
        status = status[random.randint(0, 1)],
        cost = random.uniform(0, 30),
    )

def report(err, msg):
    if err is not None:
        log.error(f"Message delivery failed {msg.key()}")
    else:
        log.info(f"Message delivered {msg.key()}")

def stream_data_to_kafka(t, topic_name):
    t=int(t)
    row = 1
    log.info("streaming data")
    while t:
        data = generate_data()
        log.info(f"generated data: {data}")
        try:
            producer.produce(
                topic=topic_name,
                key=data["user_id"],
                value=json.dumps(data).encode("utf-8"),
                on_delivery=report
            )
            # producer.flush(timeout=10.0)
            log.info(f"Data {row} loaded")
        except Exception as e:
            log.error(f"Error on transaction: {e}")
        finally:
            log.info(f"{row}. {data}")
            t -= 1
            row+=1
    start = time.time()
    producer.flush(timeout=60.0)  # Increase to 60 seconds
    log.info(f"Flush completed in {time.time() - start:.2f} seconds")
    log.info("task completed")
    log.info("task completed")
    