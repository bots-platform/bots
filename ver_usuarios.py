import sqlite3

conn = sqlite3.connect("app.db")
cursor = conn.cursor()
for row in cursor.execute("SELECT id, username, email, is_admin FROM users;"):
    print(row)
conn.close()