from flask import Flask, request
from person_service import *


app = Flask(__name__)

@app.route("/worktime", methods=["POST"])
def insert_worktime():
    try:
        # Read the json package 
        data = request.get_json()

        # Write json info to individual variables in memory
        startTime = data['startTime']
        endTime = data['endTime']
        lunchBreak = data['lunchBreak']
        consultantName = data['consultantName']
        customerName = data['customerName']

        print(data)
        #  db_add_worktime()

        return {"success": "created worktime booking: %s" % username}
    except:
        return {"error": "error creating worktime booking"}

@app.route('/worktime', methods=['GET'])
def get_all_person():
    try:  
        return db_get_persons()
    except:
        return {"error": "no data"}

if __name__ == "__main__":
    app.run(debug=True)










