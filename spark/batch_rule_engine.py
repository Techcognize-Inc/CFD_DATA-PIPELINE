import os
import psycopg2


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(base_dir, "sql", "generate_alerts.sql")

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="bankingdb",
        user="frauduser",
        password="fraudpass"
    )

    cur = conn.cursor()

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()

    print("Fraud alerts generated successfully")


if __name__ == "__main__":
    main()