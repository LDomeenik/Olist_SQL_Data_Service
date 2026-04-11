from db import get_connection

def main():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT VERSION();")
    version = cursor.fetchone()

    print("MySQL connected succesfully.")
    print("Version: ", version[0])

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()