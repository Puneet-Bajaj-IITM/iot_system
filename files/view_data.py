import sqlite3
import tabulate

def view_data(db_file, topic):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {topic}")
    rows = c.fetchall()
    conn.close()
    headers = [description[0] for description in c.description]
    print(tabulate.tabulate(rows, headers=headers, tablefmt="grid"))
import sys
db_file = "sensor_data.db"
topic =  sys.argv[1] if len(sys.argv) > 1 else 'temperature'
view_data(db_file, topic)
