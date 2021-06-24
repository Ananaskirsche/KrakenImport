import os.path
import configparser
import psycopg2
import psycopg2.extras


CONFIG_NAME = "krakenimport.conf"
REWARD_FILE_BASE_NAME = "staking_rewards_"


# Opens the DB Connection
def openDBConnection(config):
    conn = None
    dbport = 0
    try:
        dbport = int(config['DB_PORT'])
    except ValueError:
        print("DB_PORT in config is not an integer value!")

    try:
        conn = psycopg2.connect(dbname=config['DB_NAME'],
                                user=config['DB_USER'],
                                password=config['DB_PWD'],
                                host=config['DB_HOST'],
                                port=dbport)
    except Exception:
        print("Error occured when connecting to database and executing SQL!")

    return conn


# Checks if the rewards table exists and creates if it not exists
def checkCreateTable(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_name = 'rewards';")
        tablecount = cursor.fetchone()[0]

        if tablecount != 0:
            # Table seems to exist, we can continue
            return True
        # Table does not exist, so we gonna create it
        try:
            cursor.execute("""
                CREATE TABLE rewards(
                ledger_id VARCHAR(19) NOT NULL,
                asset VARCHAR(10) NOT NULL,
                distributed TIMESTAMP NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                balance DOUBLE PRECISION NOT NULL,
                PRIMARY KEY(ledger_id));
            """)
            cursor.close()
            conn.commit()
        except Exception:
            print("Failed to create rewards table!")
            return False
    return True


def getLastEntryInDB(conn, currency):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT TO_CHAR(distributed, 'YYYY-MM-DD\"T\"HH24:MI:SS') AS earliest_date FROM rewards WHERE asset = %s ORDER BY distributed DESC FETCH FIRST ROW ONLY;",
            [currency]
        )
        # If we do not have any entries, last date is None
        if cursor.rowcount == 0:
            return None
        result = cursor.fetchone()
        return result[0]


# Parses the config and returns it
def getConfig():
    parser = configparser.ConfigParser()
    parser.read(CONFIG_NAME)
    if not ("Common" in parser.sections()):
        return None
    return parser['Common']


def importWholeFile(conn, filename):
    file = open(filename, "r")
    file.readline()  # Skip CSV Header
    line = file.readline()
    rows = []
    while line:
        values = line.split(";")
        try:
            entry = (values[0], values[1], values[2], float(values[3]), float(values[4]))
            rows.append(entry)
        except Exception:
            print("Failed to parse line!")
        line = file.readline()
    with conn.cursor() as cursor:
        try:
            psycopg2.extras.execute_batch(
                cursor,
                "INSERT INTO rewards(ledger_id, distributed, asset, amount, balance) VALUES (%s, %s, %s, %s, %s)",
                rows
            )
            cursor.close()
            conn.commit()
        except Exception:
            print("Failed to run Insert SQL!")
    file.close()


def importFileBeginningFromDate(conn, filename, last_date):
    file = open(filename, "r")
    file.readline()  # Skip CSV Header
    line = file.readline()
    rows = []
    found = False
    while line:
        values = line.split(";")
        try:
            if found is False:
                if values[1] == last_date:
                    found = True
            else:
                entry = (values[0], values[1], values[2], float(values[3]), float(values[4]))
                rows.append(entry)
        except Exception:
            print("Failed to parse line!")
        line = file.readline()
    with conn.cursor() as cursor:
        try:
            psycopg2.extras.execute_batch(
                cursor,
                "INSERT INTO rewards(ledger_id, distributed, asset, amount, balance) VALUES (%s, %s, %s, %s, %s)",
                rows
            )
            cursor.close()
            conn.commit()
        except Exception:
            print("Failed to run Insert SQL!")
    file.close()


def main():
    # Check if config file exists
    if not os.path.exists(CONFIG_NAME) or not os.path.isfile(CONFIG_NAME):
        print("Could not find " + CONFIG_NAME)
        return

    config = getConfig()

    if config is None:
        print("Config file has wrong format! Please copy the default config and start over!")
        return

    conn = openDBConnection(config)
    if conn is None:
        return

    if checkCreateTable(conn) is False:
        conn.close()
        return

    currency_string = config['CURRENCIES']
    currencies = currency_string.split(",")

    for currency in currencies:
        filename = REWARD_FILE_BASE_NAME + currency + ".csv"
        # Check if file exists
        if not os.path.exists(filename) or not os.path.isfile(filename):
            print("Could not find " + filename + "! Skipping")
            continue

        last_date = getLastEntryInDB(conn, currency)
        if last_date is None:
            # No entries at all. We'll just import the whole file
            importWholeFile(conn, filename)
        else:
            # We'll import beginning from last_date
            importFileBeginningFromDate(conn, filename, last_date)
    conn.close()


if __name__ == "__main__":
    main()
