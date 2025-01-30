import psycopg2
from psycopg2.extras import RealDictCursor
from config import config
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta

# Fetch daily and weekly report data from the database
def fetch_and_write_report():
    con = None
    daily_data = []
    weekly_data = []
    cumulative_data = []
    avg_data = {}

    # Generate the last 7 days
    all_days = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    # Fetch daily report data
    daily_sql_query = """
        SELECT consultantName, customerName,
               SUM(dailyHours) AS total_hours
        FROM worktime
        WHERE startTime::date = CURRENT_DATE
        GROUP BY consultantName, customerName;
    """

    # Fetch weekly report
    weekly_sql_query = """
        SELECT consultantName, startTime::date AS work_date, 
               SUM(dailyHours) AS total_hours
        FROM worktime
        WHERE startTime >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY consultantName, startTime::date
        ORDER BY consultantName, work_date;
    """

    # Cumulative working hours grouped by customer for all consultants
    cumulative_sql_query = """
        SELECT customerName, SUM(dailyHours) AS total_hours
        FROM worktime
        GROUP BY customerName;
    """

    # Average working hours for each day for each consultant
    avg_sql_query = """
        SELECT consultantName, startTime::date AS work_date, 
               AVG(dailyHours) AS avg_hours
        FROM worktime
        WHERE startTime >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY consultantName, startTime::date
        ORDER BY consultantName, work_date;
    """

    try:
        # Load configuration for both database and storage
        db_config, storage_config = config()

        # Connect to the database
        con = psycopg2.connect(**db_config)
        cursor = con.cursor(cursor_factory=RealDictCursor)

        # Fetch daily report data
        cursor.execute(daily_sql_query)
        daily_data = cursor.fetchall()

        # Fetch weekly report data
        cursor.execute(weekly_sql_query)
        weekly_data = cursor.fetchall()

        # Fetch cumulative working hours by customer
        cursor.execute(cumulative_sql_query)
        cumulative_data = cursor.fetchall()

        # Fetch average working hours for each consultant each day
        cursor.execute(avg_sql_query)
        avg_data_raw = cursor.fetchall()

        for row in avg_data_raw:
            consultant = row['consultantname']
            if consultant not in avg_data:
                avg_data[consultant] = {}
            avg_data[consultant][row['work_date']] = row['avg_hours']

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con:
            con.close()

    # Write all reports to a single file
    today_date = datetime.now().strftime('%d-%m-%Y')
    week_start_date = (datetime.now() - timedelta(days=7)).strftime('%d-%m-%Y')
    week_end_date = datetime.now().strftime('%d-%m-%Y')

    report_filename = f"consultant_report_{today_date}.txt"

    # Daily report (hours for the day the report is fetched)
    with open(report_filename, "w") as file:
        file.write(f"Consultant Daily Report ({today_date})\n")
        file.write("=" * 54 + "\n")
        for row in daily_data:
            file.write(f"Consultant: {row['consultantname']}, Customer: {row['customername']}, Total Hours: {row['total_hours']:.2f}\n")
        
        file.write("\n")

        # Weekly report with all the past 7 days and total hours for the past 7 days
        file.write(f"\nConsultant Weekly Report ({week_start_date} - {week_end_date})\n")
        file.write("=" * 54 + "\n")

        consultants_in_weekly = set([row['consultantname'] for row in weekly_data])
        for consultant in consultants_in_weekly:
            file.write(f"Consultant: {consultant}\n")
            total_weekly_hours = 0

            # For each of the past 7 days, check if data exists for this consultant
            for day in all_days:
                # Check for data for this consultant and day, consultant matches the current consultant in the report and work_date matches the current day from all_days
                day_data = next((r for r in weekly_data if r['consultantname'] == consultant and r['work_date'] == datetime.strptime(day, '%Y-%m-%d').date()), None) # None if nothing is found
                
                if day_data:
                    file.write(f"  {day}: {day_data['total_hours']:.2f} hours\n")
                    total_weekly_hours += day_data['total_hours']
                else:
                    file.write(f"  {day}: 0.00 hours\n")
            
            file.write(f"Total hours for the week: {total_weekly_hours:.2f}\n")

            file.write("\n")

        # Cumulative working hours for all consultants, grouped by customer
        file.write("\nCumulative Working Hours Grouped by Customer\n")
        file.write("=" * 54 + "\n")
        for row in cumulative_data:
            file.write(f"Customer: {row['customername']}, Total Hours: {row['total_hours']:.2f}\n")

        # Average working hours for each day for each consultant
        file.write("\nAverage Working Hours For Each Day For Each Consultant\n")
        file.write("=" * 54 + "\n")
        for consultant, days in avg_data.items():
            file.write(f"Consultant: {consultant}\n")
            for day, avg_hours in sorted(days.items()):
                file.write(f"  {day}: {avg_hours:.2f} hours\n")
            file.write("\n")

    print(f"Report generated successfully: {report_filename}")
    return report_filename

def upload_to_blob(file_path):
    db_config, storage_config = config()

    blob_service_client = BlobServiceClient(
        account_url=storage_config['storage_account_url'],
        credential=storage_config['storage_account_key']
    )

    blob_client = blob_service_client.get_blob_client(container=storage_config['storage_container_name'], blob=file_path)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print(f"File '{file_path}' uploaded successfully.")

if __name__ == "__main__":
    report_filename = fetch_and_write_report()
    upload_to_blob(report_filename)
