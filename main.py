from faker import Faker
from faker.providers import BaseProvider
import csv

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
        "account_id"
    ]

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

def generate_data():
    file = open("cdr-data.csv", mode="w")
    file_writer = csv.writer(file)
    file_writer.writerow(header)

print(fake.numerify("+27## ### ####"))

generate_data()