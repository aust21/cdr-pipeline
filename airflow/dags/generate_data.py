import random
from datetime import timedelta
import logging
from faker import Faker
from faker.providers import BaseProvider
import csv

logger = logging.getLogger("log")

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
file = open("cdr-data.csv", mode="w")
file_writer = csv.writer(file)
file_writer.writerow(header)

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
    print("generating data")
    for indx in range(100000000):
        print(f"writing row {indx}")
        call_times = generate_call_times()
        start_time, end_time, duration = (call_times["start_time"],
                                call_times["end_time"],
                                call_times["duration"])
        cell_tower = generate_cell_tower()
        data = [
            fake.name(), fake.numerify("+27## ### ####"),
            start_time, end_time, duration, "completed",
            call_type[random.randint(0, 1)],
            status[random.randint(0, 1)],
            random.uniform(0, 30), cell_tower
        ]

        file_writer.writerow(data)
        print(f"wrote row {indx}")


if __name__ == "__main__":
    generate_data()