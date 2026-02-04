import os
import psycopg2
from dotenv import load_dotenv


load_dotenv(dotenv_path="credentials.env")

class DatabaseManager:
    def __init__(self):
        self.db_connection = psycopg2.connect(
            host="localhost",
            port="5432",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def insert_user(self, user_data):
        cursor = self.db_connection.cursor()
        query = """
            INSERT INTO users (name, email, country, city, latitude, longitude, created_at) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING user_id
        """
        # Insert user_data into table
        cursor.execute(query, (
            user_data["name"],
            user_data["email"],
            user_data["country"],
            user_data["city"],
            user_data["latitude"],
            user_data["longitude"],
            user_data["created_at"]
        ))
        user_id = cursor.fetchone()[0]

        self.db_connection.commit()  # Save changes
        cursor.close()

        return user_id

    def close_connection(self):
        if self.db_connection:
            self.db_connection.close()