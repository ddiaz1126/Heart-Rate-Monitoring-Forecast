# Heart-Rate-Monitoring-Forecast

Description:

I have utilized a POLAR H10 heart rate monitor to record a bout of exercise, and I want to visualize and forecast the heart rate data using time series analysis. The dataset is stored in a JSON file, and my analysis consists of three major components: an SQL file (HR.sql), a Python notebook for analysis and forecasting (HeartRateMonitoringForecast.ipynb), and a Tableau dashboard (HR - Athlete Profile.twbx). These components enable me to analyze correlations and patterns in the dataset and ultimately forecast future probable heart rate values.

Overview:

  1. HR.sql
In SQL, a schema (Heart_profile) is created with several tables with important inputs such as physical info, hr_zones, and exercise details. There were queries ran to view certain properties of heart rate values such as total run time and number of values in each zone.

  2. HeartRateMonitoringForecast.ipynb
Following analysis using Python, transformations (i.e Decomposition, Logarithmic, Time Shift, & Exponential Decay) were evaluated to fix randomness of the data. The best performing transformation (Logarithmic) was utilized to train three different forecasting models (AR, MA, & ARIMA).

  3. HR - Athletic Profile.twbx
An Athlete Profile has been created using Tableau to visualize the individual statistics and heart rate values during exercise sessions, categorizing them into different heart rate zones based on intensity.

