from faker import Faker
import hashlib

def create(file_path) -> None:
    fake = Faker('en_US')
    i = 0

    with open(file_path, "w") as txt_file:
        while i < 1000000:
            name = fake.name()
            id = fake.random_int(min=1, max=10000)
            id2 = fake.random_int(min=20000, max=30000)
            id3 = int(fake.day_of_month())
            id_final = hashlib.md5(str(((id*i)*id)+(id2*(id+i))+(id3*i)).encode('utf-8')).hexdigest()
            i+=1

            txt_file.write(f"""{id_final},{name}""")
            txt_file.write("\n")


if __name__ == '__main__':

    file_path = "raw/file.txt"
    
    create(file_path)