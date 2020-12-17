import os
import csv
filepath = str(os.getcwd())
input_file_location = filepath + '\Input Files'
output_location = filepath + '\Output Files'

# Initial Processing of the data, and moving it all to the output location folder. Only needs to be run once.
def run_initial_processing(question):
    if question == True:
        print('Processing players file.')
        print('Format is [nflId, height, weight].\n')
        player_input = open(input_file_location + '\players.csv','r').readlines()
        player_output = open(output_location + '\\players.csv', "w+")
        for i in range(1,len(player_input)):
            player_input[i] = player_input[i].split(',')
            if '-' in player_input[i][1]:
                player_input[i][1] = int(player_input[i][1].split('-')[0])*12 + int(player_input[i][1].split('-')[1])
            if player_input[i][5] not in ['FB', 'HB', 'QB', 'RB', 'TE', 'WR', 'P', 'K']:
                player_output.write(player_input[i][0] + ',' + str(player_input[i][1]) + ',' + player_input[i][2])
                if i != len(player_input)-2:
                    player_output.write('\n')

        print('Processing plays file.')
        print('Format is [playId, gameId, defendersInTheBox, numberOfPassRushers, offensePlayResult, playResult, passResult, epa].\n')
        play_output = open(output_location + '\\plays.csv', "w+")
        with open(input_file_location + '\plays.csv','r') as play_input:
            play_input = list(csv.reader(play_input))
            for i in range(1,len(play_input)):
                play_output.write(str(play_input[i][1]) + ',' + str(play_input[i][0]) + ',' + str(play_input[i][12]) + ',' + str(play_input[i][13]) + ',' + str(play_input[i][23]) + ',' + str(play_input[i][24]) + ',' + str(play_input[i][22]) + ',' + str(play_input[i][25]))
                if i != len(play_input)-2:
                    play_output.write('\n')

        
        print('Processing week files.')
        print('Format is [week, frameId, x, y, s, a, position, nflId, playId, gameId]')
        output_file = open(output_location + '\\tracking.csv', "w+")
        for w in range(1,18):
            print('Working on week ' + str(w) + '.')
            week = open(input_file_location + '\week' + str(w) + '.csv', "r").readlines()
            for i in range(1,len(week)):
                week[i] = week[i].split(',')
                if week[i][9] != '':
                    week[i] = [str(w)] + [week[i][13]] + week[i][1:5] + [week[i][12]] + [week[i][9]] + [week[i][16]] + [week[i][15]]
                    temp_week = ''
                    for j in range(len(week[i])):
                        temp_week += week[i][j] + ','
                    output_file.write(temp_week[:-1])
                    if i != len(week)-2:
                        output_file.write('\n')

# Comment out this line once the initial processing has been run.
run_initial_processing(True)