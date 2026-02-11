import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path


env_path = Path(__file__).resolve().parent.parent / "credentials.env"
load_dotenv(dotenv_path=env_path)


class DatabaseManager:
    # TODO: validate data before db
    def __init__(self):
        self.db_config ={
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "database": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD")
        }

    def establish_connection(self):
        """Create new database connection based on config"""
        return psycopg2.connect(**self.db_config)

    def insert_user(self, user_data):
        # TODO: validate if email is already in use, if so fail user generation.
        with self.establish_connection() as conn:
            with conn.cursor() as cursor:
                try:
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

                    conn.commit()  # Save changes

                    return user_id

                except Exception as e:
                    conn.rollback()
                    print(f"Error updating database: {e}")
                    raise

    def insert_device(self, user_device):
        with self.establish_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    query = """
                        INSERT INTO user_devices (user_id, device_type, first_used, last_used) 
                        VALUES (%s, %s, %s, %s)
                        RETURNING device_id
                    """
                    # Insert user_data into table
                    cursor.execute(query, (
                        user_device["user_id"],
                        user_device["device_type"],
                        user_device["first_used"],
                        user_device["last_used"],
                    ))
                    device_id = cursor.fetchone()[0]

                    conn.commit()

                    return device_id

                except Exception as e:
                    conn.rollback()
                    print(f"Error updating database: {e}")
                    raise


    def update_device_use_data(self, user_id, device_id):
        with self.establish_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    query = """
                        UPDATE user_devices SET 
                        last_used = %s
                        WHERE user_id = %s AND device_id = %s
                    """
                    new_last_used = datetime.now()
                    cursor.execute(query, (new_last_used, user_id, device_id)) # Update last_used to now

                    if cursor.rowcount == 0: # Check if combination of user and device id were found
                        print(f"Device {device_id} for user {user_id} was not updated.")
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print(f"Error updating database: {e}")
                    raise


    def insert_payment_method(self, user_payment_method):
        with self.establish_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    query = """
                        INSERT INTO payment_methods (user_id, payment_method, payment_service_provider)
                        VALUES (%s, %s, %s)
                        RETURNING payment_method_id
                    """
                    # Insert user_data into table
                    cursor.execute(query, (
                        user_payment_method["user_id"],
                        user_payment_method["payment_method"],
                        user_payment_method["service_provider"],
                    ))
                    payment_method_id = cursor.fetchone()[0]

                    conn.commit()

                    return payment_method_id

                except Exception as e:
                    conn.rollback()
                    print(f"Error updating database: {e}")
                    raise

    def insert_merchant(self, merchant_data):
        with self.establish_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    query = """
                        INSERT INTO merchants (merchant_name, country, rating) 
                        VALUES (%s, %s, %s)
                        RETURNING merchant_id
                    """
                    # Insert user_data into table
                    cursor.execute(query, (
                        merchant_data["name"],
                        merchant_data["country"],
                        merchant_data["rating"],
                    ))
                    user_id = cursor.fetchone()[0]

                    conn.commit()

                    return user_id

                except Exception as e:
                    conn.rollback()
                    print(f"Error updating database: {e}")
                    raise