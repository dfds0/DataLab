from flask import Flask, request
import joblib as jb
import json
import pandas as pd

app = Flask(__name__)

model = jb.load("mdl.pkl.z")

@app.route("/category_code")
def main():
    # Retrieve the tags from request
    tags = request.args.get("tags", default='')

    # Use the model to predict the result
    result = model.predict_proba([tags])[0]

    # Find the index with the highest probability of class
    result = pd.Series(result).idxmax()

    # Build the response
    res = {"tags": tags,  "category_code": int(result)}

    # Encode and return the response as json
    return json.dumps(res)

if __name__ == "__main__":
	app.run()
	