#!/usr/bin/env python3
# as of 2022-05-27

import psycopg2 as pg2

import psycopg as pg3

PG_HOST = "10.186.16.58"
PG_USER = 'postgres'
PG_PASS = 'lBwv3i4|Gb&QWqx_'
PG_DB = 'VCDB'


def main():
    print(f"Connect to remote VCDB database")
    db = None
    cur = None

    try:
        db = pg3.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS, port=5432)
        #cur = db.cursor(prepared=True)
        cur = db.cursor()
        print("connected")

    except pg3.Error as err:
        print(f"could not connect to Postgres: {err})")
        exit(1)

    """ Create table on VCDB"""
    try:
        # create a table
        sql_create = '''
            CREATE TABLE IF NOT EXISTS hello (
                id SERIAL PRIMARY KEY,
                a TEXT,
                b TEXT,
                c TEXT 
            ) 
        '''
        cur.execute(sql_create)
        print("table created")

    except pg3.Error as e:
        print(f"could not create table: {e}")
        exit(1)

    try:
        # insert rows into the table using executemany
        row_data = (
            ('one', 'two', 'three'),
            ('two', 'three', 'four'),
            ('three', 'four', 'five'),
            ('four', 'five', 'six'),
            ('five', 'six', 'seven'),
            ('six', 'seven', 'eight'),
            ('seven', 'eight', 'nine'),
            ('eight', 'nine', 'ten'),
            ('nine', 'ten', 'eleven'),
        )
        print("inserting rows")
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (%s, %s, %s)", row_data)
        count = cur.rowcount
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (%s, %s, %s)", row_data)
        count += cur.rowcount
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (%s, %s, %s)", row_data)
        count += cur.rowcount
        print(f"{count} rows added")
        db.commit()

    except pg3.Error as e:
        print(f"could not insert rows: {e}")
        exit(1)

    try:
        # count rows using SELECT COUNT(*)
        cur.execute("SELECT COUNT(*) FROM hello")
        count = cur.fetchone()[0]
        print(f"There are {count} rows in the table")

        # get column names by selecting one row and use description
        cur.execute("SELECT * FROM hello LIMIT 1")
        cur.fetchall()
        colnames = cur.description
        colname = [r[1] for r in colnames]
        print(f"column names with description: {colnames}")

        # # get column names by selecting one row and use row_factory
        # cur.execute("SELECT * FROM hello LIMIT 1")
        # cur.fetchall().row_factory
        # row = cur.description
        # colnames = [r[1] for r in row]
        # print(f"column names using row_factory: {colnames}")


        # fetch rows using iterator
        print('\nusing iterator')
        cur.execute("SELECT * FROM hello LIMIT 5")
        for row in cur:
            print(row)


        # fetch rows in groups of 5 using fetchmany
        print('\ngroups of 5 using fetchmany')
        cur.execute("SELECT * FROM hello")
        rows = cur.fetchmany(5)
        while rows:
            for r in rows:
                print(r)
            print("====== ====== ======")
            rows = cur.fetchmany(5)

        # drop table and close the database
        print('\nDrop table and close connection')
        sql_drop = '''
            DROP TABLE IF EXISTS hello
        '''
        cur.execute(sql_drop)  # cleanup the db
        cur.close()
        db.close()


    except pg3.Error as e:
        print(f"Postgres error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
