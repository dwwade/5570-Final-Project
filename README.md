# 5570-Final-Project

The output.csv file does not have a header row. The fields in the data are in the format [nflId, mean_defendersInTheBox, mean_numberOfPassRushers, mean_offensePlayResult, mean_playResult, mean_passResult_C, mean_passResult_IN, mean_passResult_I, mean_passResult_S, mean_epa].

The mean_<field> values are those that are averaged out over the number of plays the player with ID nflId was involved in.

The four mean_passResult fields are in addition, averages of the one-hot encoded values of the passResult field from the original data. That is, mean_passResult_C would be the average number of plays a player with ID nflId is involved in where the passResult is 'C', or a completed pass.
