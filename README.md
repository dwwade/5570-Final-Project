# 5570-Final-Project

The **final_project_script.py** file in the **File Preprocessing Script** folder was used to take the data within the **Input Files** subfolder, and convert it into the data within the **Output Files** subfolder.

The files within **File Preprocessing Script/Output Files** subfolder were used as the input files to the **final_project_pyspark.py** file, which in turn generated the **output.csv** file using PySpark.

The **output.csv** file does not have a header row. The fields in the data are in the format [nflId, mean_defendersInTheBox, mean_numberOfPassRushers, mean_offensePlayResult, mean_playResult, mean_passResult_C, mean_passResult_IN, mean_passResult_I, mean_passResult_S, mean_epa].
* The mean_<field> values are those that are averaged out over the number of plays the player with ID nflId was involved in.
* The four mean_passResult fields are in addition, averages of the one-hot encoded values of the passResult field from the original data. That is, mean_passResult_C would be the average number of plays a player with ID nflId is involved in where the passResult is 'C', or a completed pass.

This output.csv file was then used as the input to our linear regression model, contained within **LinearRegression.py**.
