# RunningHeartRatePrediction

Overview:
I have utilized a POLAR H10 heart rate monitor to record personal runs containing several parameters such as heart rate, pace, speed, distances, and etc. The data was downloaded from Polar Flow from January to July of 2024, however, only data from January to Febuary is used as the rest do not have heart rate.

Problem:
Due to human, mechanical, and technological errors, there are lapses of time where heart is not tracked during a run. These gaps can arise from sensor displacements caused by body movements, battery issues, or other technical malfunctions.

Solution:
A Random Forest Regression model to utilize the pace, speed, and previous heart rate parameters to predict the heart rate of lapses of times.

Structure:

  1. RunningData and test_data folders

  These folders contain CSV files on the runs from Polar Flow. The test_data contains a run, with heart rate values, that is not used to train the model and is used for testing of the prediction capabilites of the model. It is important to note, as an exercise professional (M.S), there are still a lot of uncounted measures (individual and non) that affect a run (i.e training history, weight, diet, sleep, stress, & etc.), therefore, the aim of this project is to learn based off of historical runs and the current one, how a model can predict the heart rate on just short periods of time.

  2. data_anlysis_single_session.ipynb

  This notebook analyzes an individual run and is the initial exploratory analyis of the data.

  3. HeartRatePredictions.ipynb

  This notebook contains the concatenation of data, analysis, and modeling. The model is then introduced the unseen run, ran on the first 16 minutes, and then predicts the last two minutes. 

Activate conda env: source hr_env/bin/activate