# Run for all codes
import pandas as pd


from prediction_markets.obtainContracts import main

allcontracts = main()


from buyScans import analyzeContract, smartContracts

i = 0
for smartContract in smartContracts:
        analyzeContract(smartContract, i)
        i+=1

from sellScans import analyzeSellContract, smartContracts

n = 0
for smartContract in smartContracts:
        analyzeSellContract(smartContract, n)
        n+=1



from sizedBuyers import checkPolitics

from multiBuyers import main

[correctrateofmulti, finalofchangers] = main()

print(correctrateofmulti)
print(finalofchangers)



