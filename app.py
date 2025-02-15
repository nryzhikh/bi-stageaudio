from flask import Flask, render_template, jsonify, request
import pyodbc
from datetime import datetime, timedelta

app = Flask(__name__)

# Database Connection
def get_db_connection():
    try:
        connection = pyodbc.connect("DSN=HireTrack DSN;")
        return connection
    except pyodbc.Error as e:
        app.logger.error(f"Database connection error: {e}")
        return None

# Fetch Data from Database
def fetch_data():
    conn = get_db_connection()
    if conn is None:
        return {"error": "Failed to connect to the database."}

    try:
        cursor = conn.cursor()

        sql = '''
            SELECT 
                SORT.*, 
                EQLISTS.Eql_no, 
                EQLISTS.Client_name, 
                EQLISTS.DateOut, 
                EQLISTS.Eql_title, 
                Hetype.Description AS Equipment_Name, 
                SORT.PreppedQty, 
                SORT.Quant, 
                JOBS.Job_Ref, 
                JOBS.Job_Title, 
                CASE 
                    WHEN EQLISTS.ListType = 1 THEN TRUE 
                    ELSE FALSE 
                END AS IsSubhire
            FROM SORT
            INNER JOIN EQLISTS ON SORT.Eqlno = EQLISTS.Eql_no
            INNER JOIN Hetype ON Hetype.Type = SORT.Type
            INNER JOIN JOBS ON JOBS.JobNo = EQLISTS.Job_no
            WHERE SORT.Defcon > 1
                AND EQLISTS.DateOut >= CURRENT_TIMESTAMP
                AND EQLISTS.DateOut < CURRENT_TIMESTAMP + INTERVAL '2' DAY
            ORDER BY EQLISTS.DateOut ASC;
        '''

        cursor.execute(sql)

        columns = [column[0] for column in cursor.description]
        raw_data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        grouped_data = {"Today": [], "Tomorrow": [], "Day After Tomorrow": [], "Other": []}
        total_prepped = 0
        total_items = 0

        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        day_after_tomorrow = today + timedelta(days=2)

        for item in raw_data:
            
            date_out = item['DateOut']
            if isinstance(date_out, str):
                try:
                    date_out_date = datetime.strptime(date_out, '%a, %d %b %Y %H:%M:%S %Z').date()
                except ValueError:
                    date_out_date = datetime.strptime(date_out, '%d.%m.%Y %H:%M').date()
            elif isinstance(date_out, datetime):
                date_out_date = date_out.date()
            else:
                continue

            if date_out_date == today:
                grouped_data["Today"].append(item)
            elif date_out_date == tomorrow:
                grouped_data["Tomorrow"].append(item)
            elif date_out_date == day_after_tomorrow:
                grouped_data["Day After Tomorrow"].append(item)
            else:
                grouped_data["Other"].append(item)

            total_prepped += item.get('PreppedQty', 0) or 0
            total_items += item.get('Quant', 0) or 0

        grouped_data['Total'] = {
            'Total_Prepped': total_prepped,
            'Total_Items': total_items
        }

        return grouped_data
    except pyodbc.Error as e:
        app.logger.error(f"Database query error: {e}")
        return {"error": "An error occurred while fetching data."}
    finally:
        conn.close()

# Home Route
@app.route('/')
def index():
    data = fetch_data()
    if "error" in data:
        return jsonify(data), 500
    return render_template('index.html', data=data)

# API Endpoint for Dynamic Updates
@app.route('/api/data')
def api_data():
    data = fetch_data()
    if "error" in data:
        return jsonify(data), 500
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)