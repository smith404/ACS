from flask import Flask, send_from_directory, render_template, jsonify

app = Flask(__name__)

@app.route('/')
def home():
    return "Index Page"

@app.route('/pattern/')
@app.route('/pattern/<name>')
def pattern(name=None):
    return render_template('pattern_test.html', pattern=name)

@app.route('/desktop/')
def desktop():
    return render_template('desktop.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

if __name__ == '__main__':
    app.run(debug=True)