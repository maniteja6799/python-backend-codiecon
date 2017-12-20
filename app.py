from flask import Flask, request, render_template
from tweets import processStream
import logging


app = Flask(__name__)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
query = ""


@app.route('/')
def inputKeyword():
	return render_template('input.html')

@app.route('/process', methods=['POST'])
def display():
	global query
	LOG.info("got request to process: " + request.form['query'])
	if request.form['query'] == query:
		LOG.debug("got the same request as the previous one, current request: " 
			+ request.form['query'] + " previous request: " + query)
		return render_template('input.html', message="Your request is proccessed")
	query = request.form['query']
	LOG.debug("processing request: " + query)
	processStream(request.form['query'])
	LOG.debug("returning from process method")
	return render_template('input.html', message="Your request is proccessed")

if __name__ == "__main__":
	app.run()