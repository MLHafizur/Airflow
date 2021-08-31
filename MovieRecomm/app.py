import os
import time
import pandas as pd

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

DEFAULT_ITEMS_PER_PAGE = 100

def _read_ratings(file_path):

    ratings = pd.read_csv(file_path)
    ratings = ratings.sample(n=100000, random_state=42)
    ratings = ratings.sort_values(by=['timestamp','userId', 'movieId'])

    return ratings

def _date_to_timestamp(date_str):
    if date_str is None:
        return None
    return int(time.mktime(time.strptime(date_str, '%Y-%m-%d')))

app = Flask(__name__)
auth = HTTPBasicAuth()

app.config['ratings'] = _read_ratings('/ratings.csv')
users = {os.environ['API_USER']: generate_password_hash(os.environ['API_PASSWORD'])}

@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False

@app.route('/')
def hello():
    return "Hello from the movie Rating API!"


@app.route("/ratings")
@auth.login_required
def get_ratings():
    start_date_ts = _date_to_timestamp(request.args.get('start_date', None))
    end_date_ts = _date_to_timestamp(request.args.get('end_date', None))

    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', DEFAULT_ITEMS_PER_PAGE))

    ratings_df = app.config.get('ratings')

    if start_date_ts is not None:
        ratings_df = ratings_df[ratings_df['timestamp'] >= start_date_ts]

    if end_date_ts is not None:
        ratings_df = ratings_df[ratings_df['timestamp'] <= end_date_ts]
    
    subset = ratings_df.iloc[offset:offset+limit]

    return jsonify(
        {
            "result": subset.to_dict(orient="records"),
            "offset": offset,
            "limit": limit,
            "total": ratings_df.shape[0],
        }
    )


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)