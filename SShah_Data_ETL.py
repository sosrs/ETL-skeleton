import sqlalchemy
import ETL_functions as ef


# Create connection object in SQLAlchemy to write to correct database
engine = sqlalchemy.create_engine('mysql+mysqlconnector://%s:%s@%s/%s' % (ef.user, ef.password, ef.host, ef.database))


# Create the tables in the database
# This will create a table for invalid metrics. If that is not needed, drop the 'Fails' table after running this script.
ef.create_tables(engine)


# Extract the raw data into a pandas dataframe
rawdata = ef.extract_data(ef.name_of_file)


# Transform the data into valid data with proper format.
# Also keep the invalid metrics for later inspection
# If failure data is not needed, then drop the faildata variable in the output list
[gooddata, faildata] = ef.transform_data(rawdata)


# Load valid data
ef.load_data(gooddata, engine)


# Load failed data
# If failure data is not needed, remove this line of code.
ef.load_fail_data(faildata, engine)


# Close connection when task completed
engine.dispose()
