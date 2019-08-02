from flask import Flask
from flask import escape
from flask import jsonify

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, distinct
import numpy as np
import pandas as pd

import datetime as dt

def sql_connect():

    engine = create_engine("sqlite:///Resources/hawaii.sqlite")
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    Measurement = Base.classes.measurement
    Station = Base.classes.station
    session = Session(engine)
    return engine, session, Measurement, Station

engine, session, Measurement, Station = sql_connect()


def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

app = Flask(__name__)

@app.route('/')
def homepage():
    print("Serving Home Page")
    routes = ['/api/v1.0/precepitation', '/api/v1.0/precipitation', '/api/v1.0/stations',
                '/api/v1.0/$start/$end', '/api/v1.0/$start']
    endpoints = ''
    title = '<html><body> <h1> These are the endpoints </h1>'
    for endpoint in routes:
        endpoints = endpoints + endpoint + '<br>'
    endpoints = title + endpoints + '</body> </html>'
    return endpoints

@app.route('/api/v1.0/precipitation')
def precepitation():
    print("Serving Precipitaiton")
    lastDate = session.query(func.max(Measurement.date)).first()[0]
    # Perform a query to retrieve the data and precipitation scores
    yearAgo = dt.datetime.strptime(lastDate, '%Y-%m-%d') - dt.timedelta(365)
    query = session.query(Measurement.date, Measurement.prcp).filter(func.DATE(Measurement.date) > yearAgo)
    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_df = pd.read_sql_query(query.statement , engine)
    prcp_df = prcp_df.set_index('date')
    # Sort the dataframe by date, fill na
    prcp_df = prcp_df.sort_values('date')
    prcp_df = prcp_df.fillna(value=0)
    # convert to dictionary
    prcp_dict = prcp_df.to_dict()['prcp']
    return jsonify(prcp_dict)

@app.route('/api/v1.0/stations')
def stations():
    stations_df = pd.read_sql_query(session.query(distinct(Station.name)).statement, 
                                    engine)
    return jsonify({'stations':list(stations_df['anon_1'].values)})

@app.route('/api/v1.0/tobs')
def temperature():
    lastDate = session.query(func.max(Measurement.date)).first()[0]
    yearAgo = dt.datetime.strptime(lastDate, '%Y-%m-%d') - dt.timedelta(365)
    df_station_temp = pd.read_sql_query(
                                    session.\
                                    query(Measurement.date, Measurement.station, Measurement.tobs)\
                                    .filter(func.DATE(Measurement.date) > yearAgo).statement, engine)

    return jsonify(df_station_temp.sort_values(by='date').to_dict('records'))

@app.route('/api/v1.0/<start>', defaults={'end': False})
@app.route('/api/v1.0/<start>/<end>')
def trip_temp(start, end):
    if end == False:
        end_date = session.query(func.max(Measurement.date)).first()[0]
    else:
        end_date = end

    results = calc_temps(start, end_date)[0]
    result_dict  = {"min": results[0], "average": results[1], "max": results[2]}
    return(jsonify(result_dict))


if __name__ == "__main__":
    app.run(threaded=True)

