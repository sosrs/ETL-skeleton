import pandas as pd
import sqlalchemy as sqla
from datetime import date

# Constants

# Enter the database and login information here
database = 'Everlaw'
user = 'root'
password = '1234'
host = 'localhost'

# The file is assumed to be in the same directory as the code files
name_of_file = 'dataset.csv'

# Dictionary of the data and their corresponding SQL data types, used in SQLAlchemy's loading function
dtypedict = {'user_id': sqla.types.Integer,
             'project_id': sqla.types.Integer,
             'metric_name': sqla.types.VARCHAR(45),
             'metrics_value': sqla.types.Integer,
             'day': sqla.types.Date,
             'user_org': sqla.types.Integer,
             'user_region': sqla.types.VARCHAR(3),
             'user_title': sqla.types.VARCHAR(45),
             'user_age': sqla.types.Integer,
             'project_owner': sqla.types.VARCHAR(45),
             'project_multi': sqla.types.Boolean,
             'project_purpose': sqla.types.VARCHAR(45),
             'fail_id': sqla.types.Integer}


# Functions

def create_tables(engine):
    """
    This function creates the tables in a MySQL database for this specific data loading assignment. The tables are:
    Users, Projects, Metrics, and Fails (referring to invalid metrics).

    Parameters
    ----------
    engine : a SQLAlchemy connection pool object, used to write to the correct database
    """
    meta = sqla.MetaData()
    tables = {}

    # Set up SQL command to create the Users table
    tables['users'] = sqla.Table('users', meta,
                                 sqla.Column('user_id', sqla.Integer, primary_key=True, nullable=False,
                                             autoincrement=False),
                                 sqla.Column('user_org', sqla.Integer, nullable=False),
                                 sqla.Column('user_region', sqla.VARCHAR(3)),
                                 sqla.Column('user_age', sqla.Integer),
                                 sqla.Column('user_title', sqla.VARCHAR(45)))

    # Set up SQL command to create the Projects table
    tables['projects'] = sqla.Table('projects', meta,
                                    sqla.Column('project_id', sqla.Integer, primary_key=True, nullable=False,
                                                autoincrement=False),
                                    sqla.Column('project_owner', sqla.Integer, nullable=False),
                                    sqla.Column('project_multi', sqla.BOOLEAN, nullable=False),
                                    sqla.Column('project_purpose', sqla.VARCHAR(45), nullable=False))

    # Set up SQL command to create the Metrics table
    tables['metrics'] = sqla.Table('metrics', meta,
                                   sqla.Column('metric_id', sqla.Integer, primary_key=True, nullable=False,
                                               autoincrement=True),
                                   sqla.Column('metric_name', sqla.VARCHAR(45), nullable=False),
                                   sqla.Column('metrics_value', sqla.Integer, nullable=False),
                                   sqla.Column('day', sqla.DATE, nullable=False),
                                   sqla.Column('project_id', sqla.Integer, sqla.ForeignKey('projects.project_id'),
                                               nullable=False),
                                   sqla.Column('user_id', sqla.Integer, sqla.ForeignKey('users.user_id'),
                                               nullable=False))

    # Create a table to store data of failed metrics
    # If failed metrics is not required, this table can be dropped.
    tables['fails'] = sqla.Table('fails', meta,
                                 sqla.Column('fail_id', sqla.Integer, primary_key=True, nullable=False,
                                             autoincrement=True),
                                 sqla.Column('user_id', sqla.Integer),
                                 sqla.Column('project_id', sqla.Integer),
                                 sqla.Column('metric_name', sqla.VARCHAR(45)),
                                 sqla.Column('metrics_value', sqla.Integer),
                                 sqla.Column('day', sqla.DATE, nullable=False),
                                 sqla.Column('user_org', sqla.Integer),
                                 sqla.Column('user_region', sqla.VARCHAR(3)),
                                 sqla.Column('user_title', sqla.VARCHAR(45)),
                                 sqla.Column('user_age', sqla.Integer),
                                 sqla.Column('project_owner', sqla.Integer),
                                 sqla.Column('project_multi', sqla.BOOLEAN),
                                 sqla.Column('project_purpose', sqla.VARCHAR(45)))

    # Create all four tables in the database
    meta.create_all(engine)


def extract_data(file):
    """
    This function will extract the data from the csv file into a pandas dataframe.
    It assumes the file is in the same directory as the code files.
    Currently, all failed metrics are extracted, separated, and loaded into their own table.
    If failed metrics are not of interest, I could excise some here (e.g. metrics with blank metric_value).

    Parameters
    ----------
    file: a String containing the name of the CSV file to be loaded into the database

    Returns
    -------
    Pandas dataframe with all the data from the CSV as-is.
    """
    return pd.read_csv(file)


def transform_data(dataframe):
    """
    This function transforms the extracted data so that invalid data is removed, and reformats the date to match SQL.
    If the date is missing, the function will assume the intended date is the current date. This logic can be removed by
    commenting out the first line, marked below.
    The criteria for valid data is: user_id, user_org, project_id, project_onwer, project_purpose, project_multi,
    metric_name, and metrics_value must all be present. In addition, if project_onwer != user_org, then project_multi
    must be true. Any data that doesn't meet this criteria will be moved into the invalid data table.

    Parameters
    ----------
    dataframe: A pandas dataframe containing all the data to be loaded into the SQL database.

    Returns
    -------
    gooddata: A pandas dataframe containing all of the valid metrics extracted.
    faildata: A pandas dataframe containing all of the invalid metrics.

    """
    # Fill blank dates with today's date
    # This can be removed, but then either this must be verified before ETL or the table parameters must change.
    dataframe['day'].fillna(date.today(),inplace=True)

    # Convert dates in raw data to proper format for loading into SQL DATE format
    dataframe['day'] = pd.to_datetime(dataframe['day'], dayfirst=False)

    # The first query is for the valid data. It verifies that the organization is allowed to access the data,
    # and none of the not-nullable data is null.
    gooddata = dataframe.query('(project_owner == user_org or project_multi==True)'
                               'and user_id.notnull()'
                               'and user_org.notnull()'
                               'and project_id.notnull()'
                               'and project_owner.notnull()'
                               'and project_purpose.notnull()'
                               'and project_multi.notnull()'
                               'and metric_name.notnull()'
                               'and metrics_value.notnull()', inplace=False)

    # The second query is for data that does not meet one of the validation criteria.
    faildata = dataframe.query('(project_owner != user_org and project_multi==False)'
                               'or user_id.isnull()'
                               'or user_org.isnull()'
                               'or project_id.isnull()'
                               'or project_owner.isnull()'
                               'or project_purpose.isnull()'
                               'or project_multi.isnull()'
                               'or metric_name.isnull()'
                               'or metrics_value.isnull()', inplace=False)

    return [gooddata.drop_duplicates(), faildata]


def load_data(dataframe, engine):
    """
    This function takes the valid metrics, and loads it into the tables prepared by the create_tables function.
    Note that it will append the data if the table already exists. This shouldn't come up, but would allow the function
     to be reused down the line.

    Parameters
    ----------
    dataframe: A pandas dataframe containing all of the valid metrics extracted.
    engine: A SQLAlchemy connection pool that describes the database to write to.
    """
    # Query for the user metadata, remove duplicates, and load into the SQL table
    userdata = dataframe[['user_id', 'user_org', 'user_region', 'user_age', 'user_title']].drop_duplicates()

    # Load this data into the SQL table
    userdata.to_sql('users', engine, if_exists='append', index=False, dtype=dtypedict)

    # Query for project metadata, remove duplicates, and load into the SQL table
    projectdata = dataframe[['project_id', 'project_owner', 'project_multi', 'project_purpose']].drop_duplicates()
    projectdata.to_sql('projects', engine, if_exists='append', index=False, dtype=dtypedict)

    # Query for all metric data, and load into SQL table
    metricdata = dataframe[['metric_name', 'metrics_value', 'project_id', 'user_id', 'day']]
    metricdata.to_sql('metrics', engine, if_exists='append', index=False, dtype=dtypedict)


def load_fail_data(dataframe, engine):
    """
    This function takes the invalid metrics, and loads it into the tables prepared by the create_tables function.
    Unlike the valid data load function, this will load all the data as is, because I can't be sure that the error came
    from an incorrectly entered metric or improper access. Therefore, the data is presented with no assumptions.
    Note that it will append the data if the table already exists. This shouldn't come up, but would allow the function
    to be reused down the line.

    Parameters
    ----------
    dataframe: A pandas dataframe containing all of the invalid metrics extracted.
    engine: A SQLAlchemy connection pool that describes the database to write to.
    """
    dataframe.to_sql('fails', engine, if_exists='append', index=False, dtype=dtypedict)
