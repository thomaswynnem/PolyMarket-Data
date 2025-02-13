import pandas as pd


def marketDetails():

    marketsDF = pd.read_csv("data/silver/markets_with_ai_categories.csv")

    totalBuyScansDF = pd.read_csv('data/silver/contract_buy.csv')

    marketOutcomes = pd.read_csv('data/silver/marketOutcomes.csv')

    categories = marketsDF['category'].value_counts()

    contracts = totalBuyScansDF['smartContract'].unique()

    sums = {}

    for contract in contracts:
        sums[contract] = [int(totalBuyScansDF.loc[totalBuyScansDF['smartContract'] == contract, 'investmentAmount'].sum())]


    categorySpending = {}
    for category in categories.keys():
        topicContracts = marketsDF.loc[marketsDF['category'] == category, 'marketMakerAddress'].unique()
        for contract in topicContracts:
            try:
                if category in categorySpending:
                    categorySpending[category] += int(totalBuyScansDF.loc[totalBuyScansDF['smartContract'] == contract, 'investmentAmount'].sum())
                else:
                    categorySpending[category] = int(totalBuyScansDF.loc[totalBuyScansDF['smartContract'] == contract, 'investmentAmount'].sum())
            except:
                categories[category] -= 1
                continue
        print(f"Category {category}: ${categorySpending[category]}")
    
        categorySpending[category] = [categorySpending[category] / (categories[category] * (10**6))]
    
    print(sums)
    print(categorySpending)

    totalSpent = sum(value[0] for value in sums.values() if isinstance(value, list) and len(value) > 0)

    totalContracts = len(sums)

    averageSpent = totalSpent/totalContracts

    print(averageSpent/(10**6))



    outcomes = marketOutcomes['outcome'].value_counts()

    print(outcomes)

    interestingData = {'averageSpend': [averageSpent/(10**6)], 'YesOutcome': [outcomes[0]], 'NoOutcome': [outcomes[1]]}

    categorySpending = pd.DataFrame.from_dict(categorySpending)
    sums = pd.DataFrame.from_dict(sums)
    interestingData = pd.DataFrame.from_dict(interestingData)

    categorySpending.to_csv('data/silver/allContracts/categorySpending.csv')
    sums.to_csv('data/silver/allContracts/contractSpending.csv')
    interestingData.to_csv('data/silver/allContracts/contractSplits.csv')