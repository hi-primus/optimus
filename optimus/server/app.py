from flask import Flask, jsonify

app = Flask(__name__)

path = '~//profile.json'


@app.route("/")
def profile():
    with open(path) as f:
        data = json.load(f)
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
