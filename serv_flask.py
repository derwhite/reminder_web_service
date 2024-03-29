from flask import Flask, render_template, request, redirect, url_for
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash, generate_password_hash
import duckdb
from datetime import datetime
import os
import json

db_file = "my_db.duckdb"

app = Flask(__name__)
auth = HTTPBasicAuth()
login = json.loads(open("login.json").read())


@auth.verify_password
def verify_password(username, password):
    if username in login and check_password_hash(login.get(username), password):
        return username


@app.route("/")
@auth.login_required
def index():
    return render_template("index.html")


@app.route("/new")
@auth.login_required
def new():
    return render_template("add.html")


@app.route("/get/<str_x>")
@auth.login_required
def get(str_x):
    with duckdb.connect(os.path.join(os.path.dirname(__file__), db_file)) as conn:
        if str_x == "all" or str_x == "":
            entries = conn.execute("SELECT * FROM entries order by date").df()
        else:
            str_x = f"%{str_x.lower()}%"
            entries = conn.execute(
                "SELECT * FROM entries where lower(name) like $name or lower(comment) like $comment or lower(email) like $email order by date",
                {"name": str_x, "email": str_x, "comment": str_x},
            ).df()
        entries["date"] = entries["date"].dt.strftime("%d.%m.%Y")
        entries = entries.values.tolist()
        if len(entries) == 0:
            return "No entries found"

    return render_template("table.html", entries=entries)


@app.route("/add", methods=["POST"])
@auth.login_required
def add_entry():
    # Insert a new entry into the database
    name = request.form["name"]
    comment = request.form["comment"]
    email = request.form["email"]
    date_str = request.form["date"]
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    with duckdb.connect(os.path.join(os.path.dirname(__file__), db_file)) as conn:
        ids = conn.execute("select id from entries").df()["id"].tolist()
        print("AAAAA", ids)
        if ids == []:
            id = 1
        else:
            id = next_index(ids, 1)
            print(ids, id)
        conn.execute(
            "INSERT INTO entries VALUES ($id, $name, $comment, $email, $date)",
            {"id": id, "name": name, "comment": comment, "email": email, "date": date},
        )

    return redirect(url_for("get", str_x="all"))


@app.route("/delete/<int:id>", methods=["POST"])
@auth.login_required
def delete_entry(id):
    # Delete an entry from the database
    with duckdb.connect(os.path.join(os.path.dirname(__file__), db_file)) as conn:
        conn.execute("DELETE FROM entries WHERE id = $id", {"id": id})
    return "deleted !"


def next_index(x, _from=None):
    x.sort()
    bot = 0
    if _from != None:
        if x[0] > _from:
            return _from
    top = len(x) - 1
    offset = x[0]
    if len(x) - 1 + offset == x[-1]:
        return len(x) + offset
    while True:
        pos = (bot + top) // 2
        if pos + offset != x[pos]:
            top = pos
        elif pos + offset == x[pos]:
            bot = pos
        if top - bot == 1:
            return top + offset


if __name__ == "__main__":
    # Create the database table if it doesn't exist
    with duckdb.connect(os.path.join(os.path.dirname(__file__), db_file)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, name VARCHAR, comment VARCHAR, email VARCHAR, date DATE)"
        )
    # Run the Flask app
    app.run(debug=True)