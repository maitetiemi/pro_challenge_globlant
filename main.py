from flask import Flask, g, jsonify
from flask_restful import Resource, Api
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from fastavro import writer, reader, parse_schema
from datetime import datetime
import json


# Create the flask app
app = Flask(__name__)

app.path = "data/origin"
app.secret_key = "base64:Nml3enB0cndlbTF1NXFodjZ5Y211ODh5NjN6MHg1c2k="
app.engine = create_engine(
    "postgresql+psycopg2://postgres:qwe123@localhost/dev")
# Create an API object
api = Api(app)

# Class for Connection with database


class Connect():
    # function to connect
    def get_db_connection(self):
        conn = psycopg2.connect(host='localhost',
                                database="dev",
                                user="dev_user",
                                password="qwe123")
        return conn
    # function to insert

    def insert_validate(self, table, df):
        # load in to database, replace if exists
        df.to_sql(table, con=app.engine, if_exists='replace',
                  index_label='id', index=False)
        # lets validate de insert
        g.db = self.get_db_connection()

        cursor = g.db.cursor()
        # how mane rows are inserted
        query = "SELECT COUNT(*) FROM " + table
        cursor.execute(query)
        results = cursor.fetchone()
        g.db.close()
        return results

    def insert_log(self, table, transaction, rows):

        df = pd.DataFrame({"transaction": transaction, "intable": table,
                          "rows": rows, "datetime": datetime.now()})

        df.to_sql("log", con=app.engine, if_exists='append', index=False)
        g.db = self.get_db_connection()

        cursor = g.db.cursor()
        # how mane rows are inserted
        query = "SELECT COUNT(*) FROM log"
        cursor.execute(query)
        results = cursor.fetchone()
        g.db.close()

        if results == 1:
            return "Transaction was inserted on table log successfuly"
        else:
            return "Was a error while insert the transaction in table log "

# Class to Get data from CSV


class GetData(Resource):

    def get_read_files(self, table, kwargs):
        # read the files
        data = pd.read_csv(delimiter=",", **kwargs)
        return data

    # Function to get data from job files
    def get_job(self):
        col = ["id", "job"]
        path = app.path + "/jobs.csv"
        kwargs = {"filepath_or_buffer": path, "names": col}
        df = self.get_read_files("job", kwargs)
        c = Connect()
        results = c.insert_validate("job", df)

        c.insert_log("job", "Extract CSV", results)
        j = {"job": {'message': 'The below specified data added to database',
                     'count_rows': results}}
        return j

    # Function to get data from departments file
    def get_departments(self):
        col = ["id", "departments"]
        path = app.path + "/departments.csv"
        kwargs = {"filepath_or_buffer": path, "names": col}
        df = self.get_read_files("departments", kwargs)
        c = Connect()
        results = c.insert_validate("departments", df)
        c.insert_log("departments", "Extract CSV", results)
        j = {"departments": {
            'message': 'The below specified data added to database', 'count_rows': results}}
        return j

    # Function to get data from hired_employees files
    def get_hired_employees(self):
        col = ["id", "name", "datetime", "department_id", "job_id"]
        path = app.path + "/hired_employees.csv"
        kwargs = {"filepath_or_buffer": path, "names": col,
                  "parse_dates": ["datetime"], "infer_datetime_format": "True"}
        df = self.get_read_files("hired_employees", kwargs)
        df = self.transform_hired_employees(df)
        c = Connect()
        results = c.insert_validate("hired_employees", df)
        c.insert_log("hired_employees", "Extract CSV", results)
        j = {"hired_employees": {
            'message': 'The below specified data added to database', 'count_rows': results}}
        return j

    # Function to transform data tyoe from some columns
    def transform_hired_employees(self, df):
        df["job_id"] = df["job_id"].astype('Int64')
        df["department_id"] = df["department_id"].astype('Int64')
        return df

    # GET Request
    def get(self):
        re = {}
        re.update(self.get_job())
        re.update(self.get_departments())
        re.update(self.get_hired_employees())
        return re

# Class to backup to avro the data from database - table hired_employees


class Backup_avro_hired(Resource):
    def get(self):
        re = {}
        re.update(self.hired_employees_avro())
        return re

    def hired_employees_avro(self):
        schema = {
            "doc": "hired_empl",
            "name": "hired_employees",
            "namespace": "employee",
            "type": "record",
            "fields": [{"name": "id", "type": "long"},
                       {"name": "name", "type": ["null", "string"]},
                       {"name": "datetime", "type": "string"},
                       {"name": "department_id", "type": ["null", "float"]},
                       {"name": "job_id", "type": ["null", "float"]}
                       ],
        }
        parsed_schema = parse_schema(schema)
        dtype = {"datetime": str}
        df = pd.read_sql_query('select * from hired_employees',
                               con=app.engine, coerce_float=False, dtype=dtype)
        records = df.to_dict('records')
        results = df.count()
        c = Connect()
        c.insert_log("hired_employees", "Backup Avro", results)
        j = {"backup_hired_employees": {
            'message': "You backup as sucessfull completed on path: data/backup to table hired_employees"}}
        with open('data/backup/hired_employees.avro', 'wb') as out:
            writer(out, parsed_schema, records)

        return j

# Class to backup to avro the data from database - table job


class Backup_avro_job(Resource):
    def get(self):
        re = {}
        re.update(self.job_avro())

        return re

    def job_avro(self):
        schema = {
            "doc": "job_employees",
            "name": "job",
            "namespace": "employee",
            "type": "record",
            "fields": [{"name": 'id', 'type': 'long'},
                       {"name": 'job', 'type': 'string'}
                       ],
        }
        parsed_schema = parse_schema(schema)
        df = pd.read_sql_query('select * from job',
                               con=app.engine, coerce_float=False)
        records = df.to_dict('records')
        results = df.count()
        c = Connect()
        c.insert_log("job", "Backup Avro", results)
        with open('data/backup/job.avro', 'wb') as out:
            writer(out, parsed_schema, records)

        j = {"backup_job": {
            'message': "You backup as sucessfull completed on path: data/backup to table job"}}
        return j

# Class to backup to avro the data from database - table departments


class Backup_avro_departments(Resource):
    def get(self):
        re = {}
        re.update(self.departments_avro())
        return re

    def departments_avro(self):
        schema = {
            "doc": "departments_employees",
            "name": "departments",
            "namespace": "employee",
            "type": "record",
            "fields": [{"name": 'id', 'type': 'long'},
                       {"name": 'departments', 'type': 'string'}
                       ],
        }
        parsed_schema = parse_schema(schema)
        df = pd.read_sql_query('select * from departments',
                               con=app.engine, coerce_float=False)
        records = df.to_dict('records')
        results = df.count()
        c = Connect()
        c.insert_log("departments", "Backup Avro", results)
        with open('data/backup/departments.avro', 'wb') as out:
            writer(out, parsed_schema, records)

        j = {"backup_departments": {
            'message': "You backup as sucessfull completed on path: data/backup to table departments"}}
        return j

# Class to restore the avro file to database - table job


class Restore_backup_job(Resource):
    def get(self):
        re = {}
        re.update(self.restore_job_avro())
        return re

    def restore_job_avro(self):

        avro_records = []
        with open('data/backup/job.avro', 'rb') as fo:
            avro_reader = reader(fo)
            for record in avro_reader:
                avro_records.append(record)

        df_avro = pd.DataFrame(avro_records)
        c = Connect()
        results = c.insert_validate("job", df_avro)
        c.insert_log("job", "Restore Avro", results)
        j = {"job_avro": {
            'message': 'The below specified data have being restore to database', 'count_rows': results}}
        return j

# Class to restore the avro file to database - table departments


class Restore_backup_departments(Resource):
    def get(self):
        re = {}
        re.update(self.restore_departments_avro())
        return re

    def restore_departments_avro(self):
        avro_records = []
        with open('data/backup/departments.avro', 'rb') as fo:
            avro_reader = reader(fo)
            for record in avro_reader:
                avro_records.append(record)

        df_avro = pd.DataFrame(avro_records)
        c = Connect()
        results = c.insert_validate("departments", df_avro)
        c.insert_log("departments", "Restore Avro", results)
        j = {"departments_avro": {
            'message': 'The below specified data have being restore to database', 'count_rows': results}}
        return j

# Class to restore the avro file to database - table hired_employees


class Restore_backup_hired_employees(Resource):
    def get(self):
        re = {}
        re.update(self.restore_hired_employees_avro())
        return re

    def restore_hired_employees_avro(self):
        avro_records = []
        with open('data/backup/hired_employees.avro', 'rb') as fo:
            avro_reader = reader(fo)
            for record in avro_reader:
                avro_records.append(record)

        df_avro = pd.DataFrame(avro_records)
        df_avro["job_id"] = df_avro["job_id"].astype('Int64')
        df_avro["department_id"] = df_avro["department_id"].astype('Int64')
        df_avro["datetime"] = df_avro["datetime"].astype('datetime64')
        c = Connect()
        results = c.insert_validate("hired_employees", df_avro)
        c.insert_log("hired_employees", "Restore Avro", results)
        j = {"hired_employees_avro": {
            'message': 'The below specified data have being restore to database', 'count_rows': results}}
        return j
    
class GetCountQuarters(Resource):
    def get(self):
        c = Connect()
        cursor = c.get_db_connection().cursor()
        query = "SELECT d.departments, j.job, \
                COALESCE(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 1 THEN \
                COUNT(he.id)\
                END,0)  as ""Q1"",\
                COALESCE(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 2 THEN \
                COUNT(he.id) \
                END,0) AS ""Q2"",\
                COALESCE(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 3 THEN \
                COUNT(he.id) \
                END,0) AS ""Q3"",\
                COALESCE(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 4 THEN \
                COUNT(he.id) \
                END,0) as ""Q4""\
                from hired_employees as he\
                inner join departments as d\
                on he.department_id = d.id\
                inner join job as j\
                on he.job_id = j.id\
                where extract(YEAR FROM he.datetime) = '2021'\
                group by d.departments, j.job,EXTRACT(QUARTER FROM he.datetime)\
                order by d.departments, j.job"
        cursor.execute(query)
        res = cursor.fetchall()
        c.get_db_connection().close()
        return jsonify(res)
    
class GetCountMoreAVG(Resource):
    def get(self):
        c = Connect()
        cursor = c.get_db_connection().cursor()
        query = "with mean_all as ( select d.id,d.departments, count(he.id) hired \
                from hired_employees as he \
                inner join departments as d \
                on he.department_id = d.id \
                where extract(YEAR FROM he.datetime) = '2021' \
                group by d.id,d.departments	) \
                select id, departments, hired \
                from mean_all \
                where hired > ( select avg(hired) from mean_all) \
                order by hired desc"

        cursor.execute(query)
        res = cursor.fetchall()
        c.get_db_connection().close()
        return jsonify(res)
    
        

# Add the defined resources along with their corresponding urls
api.add_resource(GetData, '/getdata')
api.add_resource(Backup_avro_job, '/backup_avro_job')
api.add_resource(Backup_avro_departments, '/backup_avro_departments')
api.add_resource(Backup_avro_hired, '/backup_avro_hired')
api.add_resource(Restore_backup_job, '/restore_avro_job')
api.add_resource(Restore_backup_departments, '/restore_avro_departments')
api.add_resource(Restore_backup_hired_employees, '/restore_avro_hired')
api.add_resource(GetCountQuarters, '/get_count_quarters')
api.add_resource(GetCountMoreAVG, '/get_count_avg')


# Driver function
if __name__ == '__main__':

    app.run(debug=True)
