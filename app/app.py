from flask import Flask, flash, jsonify, redirect, render_template, request
from werkzeug.utils import secure_filename
from google.cloud import aiplatform
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./pacific-torus-347809-106feaa3cc83.json"
PROJECT_ID = 'pacific-torus-347809'
REGION = 'us-central1'
ENDPOINT = '7539685483194875904'

def endpoint_predict_sample(
    project: str, location: str, instances: list, endpoint: str
):
    aiplatform.init(project=project, location=location)

    endpoint = aiplatform.Endpoint(endpoint)

    prediction = endpoint.predict(instances=instances)
    return prediction

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/predict", methods=["GET", "POST"])
def predict():
    if request.method == "POST":
        input = request.form.get('input')
        try: 
            input = [[int(i) for i in input.split(', ')]]
            output = endpoint_predict_sample(PROJECT_ID, REGION, input, ENDPOINT)
            output = "True" if output == 1 else "False"
            return render_template("/output.html", output = output)
        except:
            return render_template("predict.html")
    else:
        return render_template('predict.html')

@app.route("/output", methods=["GET", "POST"])
def output():
    if request.method == "GET":
        return render_template('/output.html')
    if request.method == "POST":
        return redirect('predict.html')
