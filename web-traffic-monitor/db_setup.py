import json
import logging
from typing import List

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.root.setLevel(logging.INFO)

with open("config.json", "r") as f:
    config = json.load(f)


class DatabaseClient:
    def __init__(self):
        conn = psycopg2.connect(
            dbname=config["POSTGRES_DBNAME"],
            user=config["POSTGRES_USERNAME"],
            host=config["POSTGRES_HOST"],
            password=config["POSTGRES_PASSWORD"],
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = conn.cursor()

    def update_or_create_page(self, data: List):
        query = "SELECT * FROM pages where name=? and session=?"
        self.cursor.execute(query, data[:-1])
        result = self.cursor.fetchone()
        if result is None:
            self.create_pages(data)
        else:
            logging.info(result)
            self.update_pages(result["id"])

    def create_tables(self):
        page_query = """
            CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            session VARCHAR(255) NOT NULL,
            first_visited TIMESTAMP NOT NULL,
            visits INTEGER NOT NULL DEFAULT 1
            );
        """
        session_query = """
            CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY,
            ip VARCHAR(255) NOT NULL,
            continent VARCHAR(255) NOT NULL,
            country VARCHAR(255) NOT NULL,
            city VARCHAR(255) NOT NULL,
            os VARCHAR(255) NOT NULL,
            browser VARCHAR(255) NOT NULL,
            session VARCHAR(255) NOT NULL,
            created_at TIMESTAMP NOT NULL
            );
        """
        self.cursor.execute(page_query)
        self.cursor.execute(session_query)
        logging.info("Successfully created tables")

    def create_pages(self, data: List):
        logging.info(f"Creating page: {data}")
        query = "INSERT INTO pages(name, session, first_visited) VALUES (?, ?, ?)"
        self.cursor.execute(query, data)

    def update_pages(self, page_id: int):
        logging.info(f"Update page with: {page_id}")
        query = "UPDATE pages SET visits = visits + 1 WHERE id = ?"
        self.cursor.execute(query, [page_id])

    def create_session(self, data: List):
        query = """
            INSERT INTO sessions
             (ip, continent, country, city, os, browser, session, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         """
        self.cursor.execute(query, data)

    def select_all_sessions(self):
        query = "SELECT * FROM sessions"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def select_all_pages(self):
        query = "SELECT * FROM pages"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def select_all_user_visits(self, session_id):
        query = "SELECT * FROM pages where session = ?"
        self.cursor.execute(query, [session_id])
        rows = self.cursor.fetchall()
        return rows

    def main(self):
        self.create_tables()


if __name__ == "__main__":
    db_client = DatabaseClient()
    db_client.main()
