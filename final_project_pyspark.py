from pyspark import SparkContext

def main():

   sc = SparkContext(appName='SparkWordCount')

   def player_tracking_processing(line):
      line = line.split(',')
      if line[7] == '':
         return []
      return [(str(line[7]) + ' ' + str(line[8]) + ' ' + str(line[9]), 1)]

   def list_creation(a, b):
      if type(a) is list and type(b) is list:
         a.extend(b)
      elif type(a) is list and type(b) is not list:
         a.append(b)
      else:
         a = [a,b]
      return a
   
   def list_to_str(value):
      temp_val = 'nflId_list: '
      if isinstance(value,str):
         return temp_val + value
      for i in range(len(value)):
         if isinstance(value[i],list):
            for j in range(len(value[i])):
               temp_val += str(value[i][j]) + ' '
         else:
            temp_val += str(value[i]) + ' '
      return temp_val[:-1]

   def plays_processing(line):
      line = line.split(',')
      key = line[0] + ' ' + line[1]
      passResult = line[6]
      if passResult == 'C':
         passResult = '1 0 0 0'
      elif passResult == 'IN':
         passResult = '0 1 0 0'
      elif passResult == 'I':
         passResult = '0 0 1 0'
      elif passResult == 'S':
         passResult = '0 0 0 1'
      else:
         passResult = '0 0 0 0'
      value = 'playData: ' + line[2] + ' ' + line[3] + ' ' + line[4] + ' ' + line[5] + ' ' + passResult + ' ' + line[7] + ' 1'
      return (key,value)

   def data_fuser(a,b):
      if a[0] == 'n':
         return str(b).split(': ')[1] + ';;;' + str(a).split(': ')[1]
      else:
         return str(a).split(': ')[1] + ';;;' + str(b).split(': ')[1]
   
   def player_mapper(key,value):
      output_list = []
      value = value.replace('\n','')
      value = value.split(';;;')
      if len(value) != 2:
         return output_list
      new_value = value[0]
      new_key = value[1].split(' ')
      for i in range(len(new_key)):
         output_list.append((new_key[i], new_value))
      return output_list

   def list_counter(a,b):
      a = a.split(' ')
      b = b.split(' ')
      output_str = ''
      for i in range(len(a)):
         if a[i] == '':
            a[i] = 0
         if b[i] == '':
            b[i] = 0
         output_str += str(float(a[i]) + float(b[i])) + ' '
      return output_str[:-1]

   def averaging_fxn(value):
      value = value.split(' ')
      output_str = ''
      for i in range(len(value[:-1])):
         if value[i] == '':
            value[i] = 0
         output_str += str(float(value[i])/float(value[-1])) + ','
      return output_str[:-1]

   players = sc.textFile('/user/cloudera/input/players.csv')
   plays = sc.textFile('/user/cloudera/input/plays.csv')
   tracking = sc.textFile('/user/cloudera/input/tracking.csv')


#   player_data = players.map(lambda line: (line.split(',')[0], 'h_and_w: ' + line.split(',')[1] + ' ' + line.split(',')[2]))
#                        .map(lambda (k,v): str(k) + '\t' + str(v))
#   player_data.saveAsTextFile('/user/cloudera/output')



   play_data = plays.map(lambda line: plays_processing(line))

   player_tracking_data = tracking.flatMap(lambda line: player_tracking_processing(line)) \
                                  .reduceByKey(lambda a,b: a+b) \
                                  .map(lambda (k,v): (k.split(' ')[1] + ' ' + k.split(' ')[2], k.split(' ')[0])) \
                                  .reduceByKey(lambda a,b: list_creation(a,b)) \
                                  .mapValues(lambda val: list_to_str(val)) \
                                  .union(play_data) \
                                  .coalesce(1) \
                                  .reduceByKey(lambda a,b: data_fuser(a,b)) \
                                  .flatMap(lambda (k,v): player_mapper(k,v)) \
                                  .reduceByKey(lambda a,b: list_counter(a,b)) \
                                  .mapValues(lambda val: averaging_fxn(val)) \
                                  .map(lambda (k,v): str(k) + ',' + str(v)) \
                                  .coalesce(1)
   player_tracking_data.saveAsTextFile('/user/cloudera/output')


   sc.stop()

if __name__ == '__main__':
   main()
