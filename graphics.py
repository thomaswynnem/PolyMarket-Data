## Graphing Data for Visualization

## All Contracts Visualization First

import seaborn as sns 
import matplotlib.pyplot as plt 
import pandas as pd
import ast
import numpy as np

###################################################################################################

### Plots for Total Market Data


## Plot for Avg Spending on Each Category
data = pd.read_csv("data/silver/allContracts/categorySpending.csv")

adjusted = data.melt(var_name='categories', value_name='amounts')

print(adjusted)

sns.barplot(x='categories',y='amounts', data=adjusted)
plt.show()

## Plot for Yes vs No Outcome Markets

data = pd.read_csv("data/silver/allContracts/contractSplits.csv")

data = data.loc[:,data.columns != 'averageSpend']

adjusted = data.melt(var_name='outcome', value_name='amounts')

adjusted = adjusted[adjusted['outcome'] != 'Unnamed: 0']

adjusted.groupby(['outcome']).sum().plot(kind='pie', y='amounts')

plt.show()

## Rate of Correctness for Markets Based on Fund Distribution

data = pd.read_csv("data/silver/allContracts/correctsRates.csv")
data = data.loc[:,data.columns != 'Unnamed: 0']

cR = ast.literal_eval(data.iloc[2].to_list()[0])[0]
print(cR)
dist = ast.literal_eval(data.iloc[3].to_list()[0])[0]
print(dist)


adjusted = {'distribution': dist, 'correct_rate': cR}

adjusted = pd.DataFrame(adjusted)

print(adjusted)


sns.scatterplot(x='distribution',y='correct_rate', data=adjusted, hue = 'distribution',palette=['red', 'green', 'blue', 'yellow', 'cyan', 'purple', 'orange', 'pink', 'brown', 'black', 'grey'])

plt.show()



## Correct Rate for Value of Bet
'''
data = pd.read_csv("data/silver/allContracts/dollarly_valueDF.csv")
# manually chaned dollarly_valueDF to have ints instead of intervals
print(data)

data = data.loc[:,data.columns != 'Unnamed: 0']

data = data.iloc[[1]]

data = data.melt(var_name='dollarly_value', value_name='correct_rate')

data = data.dropna()

print(data)

data['dollarly_value'] = data['dollarly_value'].astype(int) + 100


sns.scatterplot(x='dollarly_value',y='correct_rate', data=data, hue = 'dollarly_value',palette=['red', 'green', 'blue', 'yellow', 'cyan', 'purple', 'orange', 'pink', 'brown', 'black', 'grey'])


plt.show()
'''

## Correct Rate for Time of Bet

data = pd.read_csv("data/silver/allContracts/timeline_quarterDF.csv")

data = data.loc[:,data.columns != 'Unnamed: 0']

data = data.iloc[[1,2]]

newdata = {'bet_time' : ['Q1','Q2','Q3','Q4'], 'correct_amounts': data.iloc[0]*data.iloc[1].values, 'incorrect_amounts': data.iloc[0] - (data.iloc[0]*data.iloc[1]).values}
newdata = pd.DataFrame(newdata) 
print(newdata)
pivot_data = newdata.set_index('bet_time').T
sns.heatmap(data = pivot_data, annot=True, fmt="g", cmap='viridis')


plt.show()

## Correct Rate for Time of Big Bet (Over 1000)

data = pd.read_csv("data/silver/allContracts/timeline_quarterDF.csv")

data = data.loc[:,data.columns != 'Unnamed: 0']

data = data.iloc[[3,4]]

newdata = {'bet_time_over_1000' : ['Q1','Q2','Q3','Q4'], 'correct_amounts': data.iloc[0]*data.iloc[1].values, 'incorrect_amounts': data.iloc[0] - (data.iloc[0]*data.iloc[1]).values}
newdata = pd.DataFrame(newdata) 
print(newdata)
pivot_data = newdata.set_index('bet_time_over_1000').T
sns.heatmap(data = pivot_data, annot=True, fmt="g", cmap='viridis')


plt.show()

###################################################################################################



### Plots for Category Market Data


# import required module
import os
# assign directory
directory = 'data/silver/Subjects'

print("Checking directory:", directory)
print("Files in directory:", os.listdir(directory))
 
# iterate over files in
# that directory
fig, axes = plt.subplots(8, 3, figsize=(20, 100))

loc = 0

for filename in os.listdir(directory):
    
    f = os.path.join(directory, filename)
    print(f"Checking file: {filename}")
    print(f"Full path: {f}")
    
    if filename.strip().endswith('_quarterDF.csv'):
        name = filename.split('_')[0]
        if (name == 'nan'):
            continue
        print(f)

        data = pd.read_csv(f)


        data = data.loc[:,data.columns != 'Unnamed: 0']

        ## All Data
        totaldata = data.iloc[[1,2]]

        totaldata = pd.DataFrame(totaldata)
        totaldata = totaldata.fillna(0)     


        totaldata = {f'{name}_bet_time' : ['Q1','Q2','Q3','Q4'], 'correct_amounts': (totaldata.iloc[0].values*totaldata.iloc[1].values).astype(int), 'incorrect_amounts': (totaldata.iloc[0] - (totaldata.iloc[0]*totaldata.iloc[1]).values).astype(int)}
        totaldata = pd.DataFrame(totaldata) 
        print(totaldata)
        pivot_data = totaldata.set_index(f'{name}_bet_time').T
        sns.heatmap(data = pivot_data, annot=True, fmt="g", cmap='viridis', ax = axes[loc,0])

        


        ## Large Data
        largedata = data.iloc[[3,4]]

        largedata = {f'1000_{name}_bet_time' : ['Q1','Q2','Q3','Q4'], 'correct_amounts': (largedata.iloc[0]*largedata.iloc[1].values).astype(int), 'incorrect_amounts': (largedata.iloc[0] - (largedata.iloc[0]*largedata.iloc[1]).values).astype(int)}
        largedata = pd.DataFrame(largedata) 
        print(largedata)
        pivot_data = largedata.set_index(f'1000_{name}_bet_time').T
        sns.heatmap(data = pivot_data, annot=True, fmt="g", cmap='viridis', ax = axes[loc,1])


        topicRates = pd.read_csv(f"data/silver/Subjects/{name}_correctsRates.csv")
        correctRates = pd.DataFrame(topicRates)
        topicRates = topicRates.loc[:,topicRates.columns != 'Unnamed: 0']
        print(correctRates)

        cR = ast.literal_eval(topicRates.iloc[2].to_list()[0])[0]
        print(cR)
        dist = ast.literal_eval(topicRates.iloc[3].to_list()[0])[0]
        print(dist)



        adjusted = {'distribution': dist, 'correct_rate': cR}

        adjusted = pd.DataFrame(adjusted)

        print(adjusted)

        sns.scatterplot(x='distribution',y='correct_rate', data=adjusted, hue = 'distribution',palette=['red', 'green', 'blue', 'yellow', 'cyan', 'purple', 'orange', 'pink', 'brown', 'black', 'grey'],ax = axes[loc,2])
        loc+=1
        

plt.savefig("myfigure.png")

