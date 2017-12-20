from flask import Flask, request, render_template
from tweets import streamTweets

app = Flask(__name__)

@app.route('/')
def inputKeyword():
	return render_template('input.html')

@app.route('/process', methods=['POST'])
def display():
	print("button pressed")
	print(request.form['query'])
	streamTweets(request.form['query'])
	print("returning from process method")
	return render_template('input.html', message="Your request is proccessed")


if __name__ == "__main__":
    app.run()