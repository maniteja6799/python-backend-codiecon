from flask import Flask, request, render_template
from tweets import processStream
import logging


app = Flask(__name__)
# LOG = logging.getLogger("Server")
# LOG.setLevel(logging.DEBUG)
query = ""


@app.route('/')
def inputKeyword():
	return render_template('input.html')

@app.route('/process', methods=['POST'])
def display():
	global query
	print("got request to process: " + request.form['query'])
	if request.form['query'] == query:
		print("got the same request as the previous one, current request: " 
			+ request.form['query'] + " previous request: " + query)
		return render_template('input.html', message="Your request is proccessed")
	query = request.form['query']
	print("processing request: " + query)
	processStream(request.form['query'])
	print("returning from process method")
	return render_template('input.html', message="Your request is proccessed")

if __name__ == "__main__":
	app.run()