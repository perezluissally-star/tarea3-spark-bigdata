import pandas as pd

df = pd.read_excel("datos/Folds5x2_pp.xlsx", sheet_name="Sheet1")
df.to_csv("datos/dataset.csv", index=False)
print(df.shape)