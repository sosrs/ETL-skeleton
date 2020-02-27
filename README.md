# ETL skeleton

## Purpose
The purpose of this script is to extract, transform, and load data into a MySQL database for further analysis. My role in this project was to design the schema to load the data into, and populate it with the initial data. The project is now defunct, but I have maintained the script to reference and build on for future projects, with references to proprietary information scrubbed.

Below, for completion, I detail the parameters of my portion of the project and my design considerations.

## Instructions
1. I make use of the following libraries. Install them on whatever computer/environment you are working in. I have provided links:
   1. Pandas library: https://pypi.org/project/pandas/
   2. SQLAlchemy: https://pypi.org/project/SQLAlchemy/1.3.11/
      1. Both this and Pandas are packaged in the Anaconda distribution. It may be easier to set up an environment with that, or use the Conda package management system to only download those two libraries.
2. Move the code files (ETL_functions.py and Data_ETL_script.py) and the CSV to be loaded into
the same directory.
3. Make sure the CSV is named “dataset.csv”.
4. I used a MySQL database for my script. Ensure the database can be accessed by the script, either by editing the code or by matching my database:
   1. Open the ETL_functions.py file. Below the import statements, there are a series of variable assignments to store the database login information. Enter in the username, password, database, and database IP address as strings.
   2. Alternatively, set the username, password, and database name to “root”,”1234”, and “database”, respectively. Set up the MySQL database on the local machine.
5. Run SShah_Data_ETL.py.

## Data Parameters
The goal is to load the below data into a SQL schema of my design. 

user_id: unique ID for a platform user

project_id: unique ID for a platform project

day: the date of the occurence for this metric

metric_name: the name of a count metric associated with the platform

metric_value: the actual count of the metric

user_organization: "name" (just a number) of the organization that the user is a part of

user_region: the region where the user is based

project_owner: the "name" (just a number) of the organization that owns this project

user_age: age of the user

user_title: age of the user

project_multi: denotes whether multiple organization have access to this project

project_purpose: denotes the purpose of this project, which can be many different forms

A project is owned by a single organization, but can have users associated with it from multiple other organizations.

## Data Model and Schema definitions
### Schema
I designed a schema containing four entities: a table of Users, a table of Projects, a table of valid Metrics,
and a table of Fails that lists the failed metrics. For this project, I assumed that while data may be
incomplete, it would not be inaccurate (e.g. I would not get two different answers for whether a project
allowed multi-organization access, but I could receive a null value). Finally, I assumed that items would
be static; users would not move organizations or age.

Users lists each user that recorded a metric in the data, as well as all of the listed data that pertains to
said user. I did not enforce data completion in this table, as I would expect users might be added
through other means, and might be missing a region, for example. I assumed each user was attached to
one organization. Given more time, I would implement a check to update the Users table as their data
changed, e.g. updating age as it incremented.

Projects lists all projects affected by the metrics, and all data that pertained to the project. Looking over
the data, I determined that all the values would be required (no project should be missing an owner,
etc.), and so I enforced completion of the table.

Metrics lists the metric itself, with date, name, and value. In addition, it contains the relevant user and
project as foreign keys, and I have added an ID column to be used as the metrics’ primary key. Since I
was given a relatively small list of data, for ease of design I placed all the metrics into one table. If the
scale were to increase dramatically, I might split up the Metrics by type, to speed up queries by not
searching through data types that weren’t explicitly joined in. Another advantage to splitting up the
metrics into multiple tables is that different data might have different security restrictions on it.
However, I considered that beyond the scope of this problem.

Fails lists any rows that did not meet the validation criteria. The validation criteria are: 

* user_id, user_org, project_id, project_owner, project_purpose, project_multi, metric_name, and metric_value must all be
present
* if project_owner was not the same as user_org, then project_multi must be true. (permission must be granted)

I chose these criteria because it seemed that they gave the metric the context it needed to be meaningful; they
can be adjusted depending on the analysts’ needs. I created a table in case analysts would want to review these failed metrics, whether
to keep track of them to identify the error or to attempt to reconstruct the metric. You may note that while I include user_id
and project_id as columns, I have not set them to foreign keys. This is because currently my only source
of valid projects and users is valid data, and if one of these entities provides no valid data I do not add it
to their respective tables. If another source could verify these entity lists, I could instate the foreign key
constraint. If storing the failed metrics is not necessary or feasible, it can easily be excised, with
comments in the code as to where.

### Design Considerations
I set up my schema to describe the 3 main entities that seemed to be of interest in this dataset (and
failures, which I wasn’t sure would or would not be of interest). Each metric is tagged to its relevant user
and project, and analysts can easily join their tables to fit to their models. That being said, there are two
more potential entities that I could consider adding to the schema: types of metrics, and organizations.
As is, I see no metadata about them that could not be pulled from joins of existing tables, but should
more metrics be introduced that described them, I would see the need to build up their own tables. If
users could be attached to multiple organizations, I would create a junction table between the two
entities as well.

I should note that the scale of this problem allowed easy manipulation of the data in RAM using Pandas
before loading to the database, so I did so. If the amount of data handled by this script were to increase
to the point that RAM could not handle it, I would either extract the raw data directly to the database
and perform my transform and load functions in the SQL environment, reasoning that hard drives are
cheap, or I would have to create a choke in the extract function so only a fraction of the data is handled
at a time.
