from flask import Flask
import json
from flask import jsonify

app = Flask(__name__)

path = 'c://optimus/profile.json'


@app.route("/")
def profile():
    with open(path) as f:
        data = json.load(f)
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
