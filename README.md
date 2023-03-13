# Challenge #1
## This script is to solve the following requirements:

- Move historic data from files in CSV format to the new database.
- Create a Rest API service to receive new data. This service must have:
- Each new transaction must fit the data dictionary rules.
- Be able to insert batch transactions (1 up to 1000 rows) with one request.
- Receive the data for each table in the same service.
- Keep in mind the data rules for each table.
- Create a feature to backup for each table and save it in the file system in AVRO format.
- Create a feature to restore a certain table with its backup.

# Requirements
## Database
PostgreSQL 15.2
## Python
Version 3.9

# Endpoints
* ***getdata*** - This endpoint extracts the data in csv and inserts it into the database
* ***backup_avro_job*** - This endpoint backs up the table job in avro file
* ***backup_avro_departments*** - This endpoint backs up the table departments in avro file
* ***backup_avro_hired*** - This endpoint backs up the table hired_employees in avro file
* ***restore_avro_job*** - This endpoint restores the avro file for table job to the database
* ***restore_avro_departments*** - This endpoint restores the avros files for table departments to the database
* ***restore_avro_hired*** - This endpoint restores the avros files for table hired_employees to the database

# Challenge #2
# Requirements
You need to explore the data that was inserted in the first challenge. The stakeholders ask for
some specific metrics they need. You should create an end-point for each requirement.

# Endpoints
* ***get_count_quarters*** - Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job.

* ***get_count_avg*** - List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments, ordered
by the number of employees hired (descending).
