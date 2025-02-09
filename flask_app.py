from flask import Flask, send_from_directory, render_template, jsonify

app = Flask(__name__)

@app.route('/')
def home():
    return "Index Page"

@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', person=name)

@app.route('/desktop/')
def hello():
    return render_template('desktop.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True)