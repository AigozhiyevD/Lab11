import psycopg2 as p
import csv
import os

# Connect to the database
con = p.connect(
    host="localhost",
    dbname="phonebook",
    user="postgres",
    password="1",
    port=5432
)
cur = con.cursor()

# Create table and stored functions/procedures
cur.execute("""
    CREATE TABLE IF NOT EXISTS phonebook(
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        surname VARCHAR(100) NOT NULL,
        phone VARCHAR(15) NOT NULL
    );

    -- Function to search records by pattern
    CREATE OR REPLACE FUNCTION search_phonebook(pattern VARCHAR)
    RETURNS TABLE (
        id INTEGER,
        name VARCHAR,
        surname VARCHAR,
        phone VARCHAR
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT p.id, p.name, p.surname, p.phone
        FROM phonebook p
        WHERE p.name ILIKE '%' || pattern || '%'
           OR p.surname ILIKE '%' || pattern || '%'
           OR p.phone ILIKE '%' || pattern || '%';
    END;
    $$ LANGUAGE plpgsql;

    -- Procedure to insert or update a user
    CREATE OR REPLACE PROCEDURE insert_or_update_user(
        p_name VARCHAR,
        p_phone VARCHAR
    ) AS $$
    BEGIN
        IF EXISTS (SELECT 1 FROM phonebook WHERE name = p_name) THEN
            UPDATE phonebook SET phone = p_phone WHERE name = p_name;
        ELSE
            INSERT INTO phonebook (name, surname, phone)
            VALUES (p_name, '', p_phone);
        END IF;
    END;
    $$ LANGUAGE plpgsql;

    -- Procedure to insert multiple users with phone validation
    CREATE OR REPLACE FUNCTION insert_many_users(
        users TEXT[][]
    ) RETURNS TEXT[] AS $$
    DECLARE
        incorrect_data TEXT[];
        user_record TEXT[];
        phone_text TEXT;
        i INT := 1;
    BEGIN
        incorrect_data := '{}';
        
        FOREACH user_record SLICE 1 IN ARRAY users
        LOOP
            IF array_length(user_record, 1) < 2 THEN
                incorrect_data := array_append(incorrect_data,
                    format('User %s: Missing name or phone', i));
            ELSE
                phone_text := user_record[2];
                
                -- Validate phone (at least 5 digits)
                IF phone_text ~ '^[0-9]{5,}$' THEN
                    PERFORM insert_or_update_user(user_record[1], phone_text);
                ELSE
                    incorrect_data := array_append(incorrect_data,
                        format('User %s: %s - Invalid phone format', i, phone_text));
                END IF;
            END IF;
            i := i + 1;
        END LOOP;
        
        RETURN incorrect_data;
    END;
    $$ LANGUAGE plpgsql;

    -- Function for paginated querying
    CREATE OR REPLACE FUNCTION get_paginated_phonebook(
        p_limit INTEGER,
        p_offset INTEGER
    ) RETURNS TABLE (
        id INTEGER,
        name VARCHAR,
        surname VARCHAR,
        phone VARCHAR
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT p.id, p.name, p.surname, p.phone
        FROM phonebook p
        ORDER BY p.name
        LIMIT p_limit OFFSET p_offset;
    END;
    $$ LANGUAGE plpgsql;

    -- Procedure to delete by username or phone
    CREATE OR REPLACE PROCEDURE delete_by_username_or_phone(
        search_term VARCHAR
    ) AS $$
    BEGIN
        DELETE FROM phonebook
        WHERE name = search_term OR phone = search_term;
    END;
    $$ LANGUAGE plpgsql;
""")
con.commit()

# Insert data from a CSV file
def insert_from_csv(filename):
    try:
        with open(filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                if len(line) != 3:
                    print(f"Skipping invalid row: {line}")
                    continue
                cur.execute(
                    "INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)",
                    (line[0], line[1], line[2])
                )
            con.commit()
            print(f"Data from {filename} inserted successfully.")
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found. Please check the file name or path.")
    except Exception as e:
        print(f"Error inserting data: {e}")

# Insert data manually through the console
def insert_from_console():
    name = input("Enter name: ")
    surname = input("Enter surname: ")
    phone = input("Enter phone: ")
    try:
        cur.execute(
            "INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)",
            (name, surname, phone)
        )
        con.commit()
        print(f"Inserted: {name} {surname} {phone}")
    except Exception as e:
        print(f"Error inserting data: {e}")

# Search by pattern using stored function
def search_pattern(cur):
    pattern = input("Enter search pattern: ")
    try:
        cur.execute("SELECT * FROM search_phonebook(%s)", (pattern,))
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"ID: {row[0]}, Name: {row[1]}, Surname: {row[2]}, Phone: {row[3]}")
        else:
            print("No matching records found.")
    except Exception as e:
        print(f"Error searching: {e}")

# Insert or update user using stored procedure
def insert_or_update_user(cur):
    name = input("Enter name: ")
    phone = input("Enter phone: ")
    try:
        cur.execute("CALL insert_or_update_user(%s, %s)", (name, phone))
        con.commit()
        print(f"User {name} added or updated successfully.")
    except Exception as e:
        print(f"Error inserting/updating user: {e}")

# Insert many users using stored function
def insert_many_users(cur):
    print("Enter users in format 'name,phone'. One per line. Enter 'done' when finished.")
    users = []
    while True:
        user_input = input("> ").strip()
        if user_input.lower() == 'done':
            break
        users.append(user_input.split(','))
    
    if not users:
        print("No users provided.")
        return
    
    try:
        users_array = [[u[0], u[1]] for u in users if len(u) >= 2]
        cur.execute("SELECT insert_many_users(%s)", (users_array,))
        incorrect_data = cur.fetchone()[0]
        if incorrect_data:
            print("Incorrect data:")
            for item in incorrect_data:
                print(f"- {item}")
        else:
            print("All users processed successfully.")
        con.commit()
    except Exception as e:
        print(f"Error inserting users: {e}")

# Paginated querying using stored function
def get_paginated_records(cur):
    try:
        limit = int(input("Enter number of records per page: "))
        offset = int(input("Enter offset (start from record): "))
        if limit < 1 or offset < 0:
            print("Limit must be positive and offset non-negative.")
            return
        cur.execute("SELECT * FROM get_paginated_phonebook(%s, %s)", (limit, offset))
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"ID: {row[0]}, Name: {row[1]}, Surname: {row[2]}, Phone: {row[3]}")
        else:
            print("No records found in this range.")
    except ValueError:
        print("Please enter valid numbers.")
    except Exception as e:
        print(f"Error retrieving records: {e}")

# Delete by username or phone using stored procedure
def delete_by_username_or_phone(cur):
    search_term = input("Enter username or phone to delete: ")
    try:
        cur.execute("CALL delete_by_username_or_phone(%s)", (search_term,))
        con.commit()
        print(f"Deleted records matching '{search_term}'.")
    except Exception as e:
        print(f"Error deleting records: {e}")

# Main menu
def main():
    while True:
        print("\n--- PhoneBook Menu ---")
        print("1. Insert data from CSV file")
        print("2. Insert data manually via console")
        print("3. Search by pattern")
        print("4. Insert or update user (stored procedure)")
        print("5. Insert many users (stored function)")
        print("6. Get paginated records")
        print("7. Delete by username or phone (stored procedure)")
        print("8. Exit")
        
        choice = input("Enter your choice (1-8): ")

        if choice == "1":
            filename = input("Enter the path of the CSV file: ")
            if not os.path.exists(filename):
                print(f"Error: File '{filename}' not found.")
            else:
                insert_from_csv(filename)
        elif choice == "2":
            insert_from_console()
        elif choice == "3":
            search_pattern(cur)
        elif choice == "4":
            insert_or_update_user(cur)
        elif choice == "5":
            insert_many_users(cur)
        elif choice == "6":
            get_paginated_records(cur)
        elif choice == "7":
            delete_by_username_or_phone(cur)
        elif choice == "8":
            print("Exiting...")
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    try:
        main()
    finally:
        cur.close()
        con.close()