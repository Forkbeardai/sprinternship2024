import snowflake.connector
import exrex
import os

class SnowflakeConnector:
    # This class is empty for now and can be expanded later
    pass
class RegexGenerator:
    def __init__(self):
        self.conn = None
        self.account = None
        self.user = None
        self.password = None
        self.warehouse = None
        self.database = None
        self.schema = None
        
    def get_credentials(self):
        # Prompting for each credential and storing them in instance variables
        self.account = input("Enter account: ")
        self.user = input("Enter user name: ")
        self.password = input("Enter password: ")
        self.warehouse = input("Enter warehouse: ")
        self.database = input("Enter database: ")
        self.schema = input("Enter database schema: ")
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        return self.conn
    
    def scanner(self):
        while True:
            regex = input("Enter your regex (or type 'exit' to stop): ")
            if regex.lower() == 'exit':
                break
            else:
                try:
                    num_times_operate = int(input("Enter the number of samples to generate: "))
                    file_path = os.path.join(os.getcwd(), "generated_data.csv")

                    with open(file_path, 'w') as file:
                        for i in range(num_times_operate):
                            generated_sample = exrex.getone(regex)
                            print(f"Generated sample: {generated_sample}")
                            file.write(f"{generated_sample}\n")

                    print(f"Generated samples have been saved to {file_path}")
                    return file_path
                except ValueError:
                    print("Please enter a valid integer for the number of samples.")
                except IOError as e:
                    print(f"Error writing to the file: {e}")


#create a class for this perhaps
def create_table_and_load_data(file_path, tablename, connection):
    data = []
    with open(file_path, mode ='r')as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            data.append(row)

    #cursor is a programming construct that allows you to manage a set of data returned by a query. 
    #a handle or pointer that allows you to interact with the result set row by row.
    # Create a cursor
    cursor = connection.cursor()

    create_table_query = f"CREATE TABLE {tablename} (regex_pattern VARCHAR)"
    cursor.execute(create_table_query, data)

    insert_query = f"INSERT INTO {tablename} (regex_pattern) VALUES (%s)"
    cursor.executemany(insert_query, data)

    connection.commit()



def main():
    generator = RegexGenerator()
    connection = generator.get_credentials()
    filename = generator.scanner()
    #table name still not asked

main()