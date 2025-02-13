import pandas as pd
import os

from bettorCorrectRate import correctPerc

from marketCorrectDistribution import correctIntervals

from marketCorrectRate import correctContracts

from sizedBuyersDistribution import intervalPerc

from timelineCorrectness import contractSwitcher

from largeInvQuarters import contractSizedSwitcher

from timeLineAmounts import contractAmountSwitcher

from contracts import marketDetails

from categories import runCategories

from aifixer import aifixer


if not os.path.exists('data/silver/allContracts'):
    os.mkdir('data/silver/allContracts')

### Collect Data
filepath = 'data/silver/markets_with_ai_categories.csv'
if not os.path.exists(filepath):
    marketsDF = pd.read_csv('data/silver/markets.csv')
    marketsDF = aifixer(marketsDF)
else:
    marketsDF = pd.read_csv(filepath)


if 'predicted_category' in marketsDF:
    marketsDF = marketsDF.drop(columns='category')
    marketsDF = marketsDF.rename(columns={'predicted_category': 'category'})
    marketsDF.to_csv(filepath)
    print("All Clear")

marketOutcomes = pd.read_csv('data/silver/marketOutcomes.csv')

totalBuyScansDF = pd.read_csv('data/silver/contract_buy.csv')
###

### CorrectsRates
filepath1 = 'data/silver/allContracts/correctsRates.csv'

if not os.path.exists(filepath1):
    bettorCorrectRate, totalBuys, totalCorrect = correctPerc(totalBuyScansDF, marketOutcomes)

    contractCorrectRates, intervals = correctIntervals(totalBuyScansDF, marketOutcomes)

    contractCorrectRate = correctContracts(totalBuyScansDF, marketOutcomes)

    correctRateDF = pd.DataFrame([bettorCorrectRate,contractCorrectRate,[contractCorrectRates],[intervals]], index=["BettorCorrectRate", "contractCorrectRate","cCRbyDistr","Distribution"])
    correctRateDF.to_csv(filepath1)
###

### timeline_quarterDF
path2 = 'data/silver/allContracts/timeline_quarterDF.csv'

if not os.path.exists(path2):
    quarterCorrect, quarterTotal = contractSwitcher(totalBuyScansDF, marketOutcomes)
    quarterSizedCorrect, quarterSizedTotal = contractSizedSwitcher(totalBuyScansDF, marketOutcomes, 1500)
    quarterAmounts = contractAmountSwitcher(totalBuyScansDF)

    quarterPerc = []
    quarterSizedPerc = []
    for iv in range(4):

        quarterPerc.append(quarterCorrect[iv]/quarterTotal[iv])
        quarterSizedPerc.append(quarterSizedCorrect[iv]/quarterSizedTotal[iv])


    timelineDF = pd.DataFrame([quarterAmounts, quarterTotal, quarterPerc, quarterSizedTotal, quarterSizedPerc], index=["Money","Bets", "PercC", "BigBets","BigPercC"], columns=["Quarter1", "Quarter2", "Quarter3", "Quarter4"])
    timelineDF.to_csv(path2)
###

### valueDF
path3 = 'data/silver/allContracts/valueDF.csv'
if not os.path.exists(path3):
    intervalBracket, correctRate, buygr = intervalPerc(totalBuyScansDF, marketOutcomes)
    dollarlyDF = pd.DataFrame([buygr, correctRate], index=["Number of Bets", "Correct Rate"], columns=["0", "100", "200","300","400","500","600","700","800","900",])
    dollarlyDF.to_csv(path3)
###


marketDetails()

runCategories()
