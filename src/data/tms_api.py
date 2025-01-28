from flask import Flask, request, jsonify
from tms_service import db_add_worktime, db_get_all_worktimes, db_get_worktime_by_id, db_get_all_cumulative, db_delete_worktime

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


        # Get feedback from result
        result = db_add_worktime(startTime, endTime, lunchBreak, consultantName, customerName)
        print(result)
        return result
    
    except Exception as e:
        print("Error details:", str(e))
        return {"error": f"Error creating worktime booking: {str(e)}"}
    

@app.route("/worktime", methods=["GET"])
def get_all_worktimes():
    try:
        # Fetch all worktimes
        worktimes = db_get_all_worktimes()
        return jsonify(worktimes), 200

    except Exception as e:
        print("Error details:", str(e))
        return jsonify({"error": f"Error fetching worktimes: {str(e)}"}), 500


@app.route("/worktime/<int:id>", methods=["GET"])
def get_worktime_by_id(id):
    try:
        # Fetch worktime by ID
        worktime = db_get_worktime_by_id(id)
        if worktime:
            return jsonify(worktime), 200
        else:
            return jsonify({"error": f"Worktime with ID {id} not found"}), 404

    except Exception as e:
        print("Error details:", str(e))
        return jsonify({"error": f"Error fetching worktime: {str(e)}"}), 500

@app.route("/cumulative", methods=["GET"])
def get_all_cumulative():
    try:
        # Fetch all cumulative records
        cumulative_records = db_get_all_cumulative()
        return jsonify(cumulative_records), 200

    except Exception as e:
        print("Error details:", str(e))
        return jsonify({"error": f"Error fetching cumulative records: {str(e)}"}), 500
    
@app.route("/worktime/<int:id>", methods=["DELETE"])
def delete_worktime(id):
    try:
        # Delete the worktime record by ID
        result = db_delete_worktime(id)
        print(result)
        return result, 200

    except Exception as e:
        print("Error details:", str(e))
        return jsonify({"error": f"Error deleting worktime record: {str(e)}"}), 500


if __name__ == "__main__":
    app.run()










