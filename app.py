#######################################################
# Module 10 Challenge - SQLAlchemy
# Name: Thet Win
# Date: July 15, 2024
# 
# app.py
# This program uses SQLAlchemy to carry out Data Analysis on the climate of Honolulu Hawaii.
# The program also analyzes a list of Stations
# The program then displays the analysis on a Website
#
#######################################################

import pandas as pd
import datetime as dt
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
# engine = create_engine("sqlite:///titanic.sqlite")
engine = create_engine("sqlite:///Module 10 - Advanced SQL/assignment/Starter_Code/Resources/hawaii.sqlite")


# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
Measurement = Base.classes.measurement
print("database connected")
#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt; (Usage: Enter Start Date - /api/v1.0/2016-08-18)<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt; (Usage: Enter Start/End Date - /api/v1.0/2016-08-18/2017-08-17)"
    )

# Define route for precipitation
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Design a query to retrieve the last 12 months of precipitation data
    # Find the most recent date in the data set.
    max_dt=session.query(func.max(Measurement.date))[0][0]
    max_dt_strp = dt.datetime.strptime(max_dt, "%Y-%m-%d")
    
    # Calculate the previous year date from the last date in data set.
    prev_year_strp = max_dt_strp - dt.timedelta(days=365)
    
    # Format the date to - YYYY-MM-DD
    prev_year_formatted = prev_year_strp.strftime("%Y-%m-%d")
    
    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date>=prev_year_formatted).all()

    # Store precipitation results to a dictionary
    prec_dict = {date: prcp for date, prcp in results}

    # Return the JSON of the dictionary
    return jsonify(prec_dict)

# Define route for Stations
@app.route('/api/v1.0/stations')
def stations():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query for active stations
    active_stations=session.query(Measurement.station, 
   func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()


    # Created a list for active stations
    active_stations_li = [{'station': station, 'count': count} for station, count in active_stations]

    # Return the JSON for the list
    return jsonify(active_stations_li)

# Define route for tobs
@app.route('/api/v1.0/tobs')
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query the last 12 months of temperature observation data (tob) for the most active station
    # Determine Most active station
    most_actv_stn = active_stations=session.query(Measurement.station, func.count(Measurement.station)). \
    group_by(Measurement.station). \
         order_by(func.count(Measurement.station). \
                  desc()).first()[0]
    
    # Using the most active station id
    # Calculate latest date for station==most_actv_stn
    max_dt_station=session.query(func.max(Measurement.date)).filter(Measurement.station==most_actv_stn).scalar()
    # Calculate the previous year date from latest date for station=='USC00519281'
    prev_yr_sation = dt.datetime.strptime(max_dt_station, '%Y-%m-%d') - dt.timedelta(days=365)
    # Format the date to - YYYY-MM-DD
    prev_yr_sation_formatted = prev_yr_sation.strftime("%Y-%m-%d")

    # Query the last 12 months of temperature observation data (tob) for the most active station
    temperature_data = session.query(Measurement.date, Measurement.tobs).filter(
        Measurement.station == most_actv_stn,
        Measurement.date >= prev_yr_sation_formatted
        ).all()
    # Created a list for the temperature
    temp_list = [{'date': date, 'temperature': temp} for date, temp in temperature_data]

    # Return the JSON for temperature list
    return jsonify(temp_list)

# Define route for Starting date and End date
@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def temp_range(start, end=None):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # If only Start date is provided by user, make End date to be the latest (Maximum) date
    if not end:
        end = session.query(func.max(Measurement.date)).scalar()

    # Calculate min, avg, max for specified starting date
    temp_summary = session.query(
        func.min(Measurement.tobs),
        func.avg(Measurement.tobs),
        func.max(Measurement.tobs)
    ).filter(
        Measurement.date >= start,
        Measurement.date <= end
    ).all()

    # Store results in a list of summary
    temp_summary_list = [{'TMIN': min_temp, 'TAVG': avg_temp, 'TMAX': max_temp} for min_temp, avg_temp, max_temp in temp_summary]
    
    # Return the JSON for sumary list
    return jsonify(temp_summary_list)




if __name__ == '__main__':
    app.run(debug=True)