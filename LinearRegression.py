from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
import os
train_accuracy_list,test_accuracy_list = [],[]
for i in range(100):
    print('Running Model ' + str(i+1) + '/100.')
    pathname = str(os.getcwd()) + '\\output.csv'
    data = open(pathname, "r")
    instances = []
    for n in data:
        ns = n.split(",")
        instances.append(ns)
    train_data, test_data = train_test_split(instances, train_size=0.8)
    train_data_features = []
    train_data_label = []
    test_data_features = []
    test_data_label = []
    for test_ins in test_data:
        test_data_features.append(test_ins[1:-1])
        test_data_label.append(test_ins[-1])
    for train_ins in train_data:
        train_data_features.append(train_ins[1:-1])
        train_data_label.append(train_ins[-1])
    train_data_features_df = pd.DataFrame(train_data_features)
    train_data_label_df = pd.DataFrame(train_data_label)
    test_data_features_df = pd.DataFrame(test_data_features)
    test_data_label_df = pd.DataFrame(test_data_label)
    reg = LinearRegression()
    reg.fit(train_data_features_df, train_data_label_df)
    pred = reg.predict(test_data_features_df)
    pred2 = reg.predict(train_data_features_df)
    pred_df = pd.DataFrame(pred)
    pred2_df = pd.DataFrame(pred2)
    accuracy = mean_squared_error(test_data_label_df, pred_df)
    accuracy2 = mean_squared_error(train_data_label_df, pred2_df)
    train_accuracy_list.append(accuracy2)
    test_accuracy_list.append(accuracy)

print('Average Training MSE: ' + str(sum(train_accuracy_list)/len(train_accuracy_list)))
print('Average Testing  MSE: ' + str(sum(test_accuracy_list)/len(test_accuracy_list)))
