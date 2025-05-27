import random
from datetime import timedelta

from faker import Faker
from faker.providers import BaseProvider
import csv
import logging

log = logging.getLogger(__name__)

header = [
        "name",
        "receiver",
        "start_time",
        "end_time",
        "duration",
        "status",
        "type",
        "direction",
        "cost",
        "cell_tower",
    ]

status = ["outgoing", "incoming"]
call_type = ["voice", "video"]


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
        receiver = receiver,
        call_type = type,
        start_time = call_times["start_time"],
        end_time = call_times["end_time"],
        duration = call_times["duration"],
        cell_tower = generate_cell_tower(),
        status = status[random.randint(0, 1)],
        cost = random.uniform(0, 30),
    )



if __name__ == "__main__":
    while True:
        data = generate_data()
        # print(data)