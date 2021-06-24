import hashlib
import json
import logging
import os
from datetime import datetime
from typing import List
from urllib import request

import httpagentparser
from db_setup import DatabaseClient
from flask import Flask, jsonify, render_template, session
from pusher import Pusher

app = Flask(__name__)
app.secret_key = os.urandom(24)

with open("config.json", "r") as f:
    config = json.load(f)

pusher = Pusher(
    app_id=config["PUSHER_APP_ID"],
    key=config["PUSHER_APP_KEY"],
    secret=config["PUSHER_APP_SECRET"],
    cluster=config["PUSHER_APP_CLUSTER"],
    ssl=True,
)


class App:
    def __init__(self):
        self.db_client = DatabaseClient()
        self.user_os = None
        self.user_ip = None
        self.user_city = None
        self.user_browser = None
        self.user_country = None
        self.user_continent = None
        self.session_id = None

    def parse_visitor(self, data: List):
        self.db_client.update_or_create_page(data)
        pusher.trigger(
            "pageview",
            "new",
            {"page": data[0], "session": self.session_id, "ip": self.user_ip},
        )

    @app.before_request
    def get_analytics_data(self):
        user_info = httpagentparser.detect(request.headers.get("User-Agent"))
        self.user_os = user_info["platform"]["name"]
        self.user_browser = user_info["browser"]["name"]
        self.user_ip = (
            "72.229.28.185"
            if request.remote_addr == "127.0.0.1"
            else request.remote_addr
        )
        api = "https://www.iplocate.io/api/lookup/" + self.user_ip
        try:
            response = request.urlopen(api)
            result = response.read()
            result = json.loads(result.decode("utf-8"))
            self.user_country = result["country"]
            self.user_continent = result["continent"]
            self.user_city = result["city"]
        except Exception as e:
            logging.error(f"Couldn't find: {self.user_ip}, {e}")

        self.get_session()

    def get_session(self):
        time = datetime.now().replace(microsecond=0)
        if "user" not in session:
            lines = (str(time) + self.user_ip).encode("utf-8")
            session["user"] = hashlib.md5(lines).hexdigest()
            self.session_id = session["user"]
            pusher.trigger(
                "session",
                "new",
                {
                    "ip": self.user_ip,
                    "continent": self.user_continent,
                    "country": self.user_country,
                    "city": self.user_city,
                    "os": self.user_os,
                    "browser": self.user_browser,
                    "session": self.session_id,
                    "time": str(time),
                },
            )
            data = [
                self.user_ip,
                self.user_continent,
                self.user_country,
                self.user_city,
                self.user_os,
                self.user_browser,
                self.session_id,
                time,
            ]
            self.db_client.create_session(data=data)
        else:
            self.session_id = session["user"]

    @app.route("/about")
    def about(self):
        data = ["about", self.session_id, str(datetime.now().replace(microsecond=0))]
        self.parse_visitor(data=data)
        return render_template("about.html")

    @app.route("/dashboard")
    def dashboard(self):
        return render_template("dashboard.html")

    @app.route("/dashboard/<session_id>", methods=["GET"])
    def session_pages(self, session_id):
        result = self.db_client.select_all_user_visits(session_id=session_id)
        return render_template("dashboard-single.html", data=result)

    @app.route("/get-all-sessions")
    def get_all_sessions(self):
        data = list()
        db_rows = self.db_client.select_all_sessions()
        for row in db_rows:
            data.append(
                {
                    "ip": row["ip"],
                    "continent": row["continent"],
                    "country": row["country"],
                    "city": row["city"],
                    "os": row["os"],
                    "browser": row["browser"],
                    "session": row["session"],
                    "time": row["created_at"],
                }
            )
            return jsonify(data)


if __name__ == "__main__":
    app_client = App()
    app.run(debug=True)
