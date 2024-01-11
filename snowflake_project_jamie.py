import snowflake.connector
import exrex
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
    def scanner(self):
        while True:
            try:
                amount_of_regex = int(input("Enter the amount of regex you want (or type 'exit' to stop): "))
            except ValueError:
                print("Please enter a valid integer or type 'exit' to stop.")
                continue
            for i in range(amount_of_regex):
                regex = input(f"Enter your regex {i+1}: ")
                if regex.lower() == 'exit':
                    return
                try:
                    loop = int(input(f"Enter the amount of data you want for regex {i+1}: "))
                except ValueError:
                    print("Please enter a valid integer.")
                    continue
                for _ in range(loop):
                    print(exrex.getone(regex))
def main():
    generator = RegexGenerator()
    generator.get_credentials()
    generator.scanner()
main()
