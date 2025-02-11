import pandas as pd
import numpy as np

df = pd.read_csv("data/sample_arpae/04000002_005_202412.csv", header=0)
print("\nFile name: 04000002_005_202412.csv")
print("Describe VALORE:")
print(df['VALORE'].describe())

df = pd.read_csv("data/sample_arpae/04000022_005_202412.csv", header=0)
print("\nFile name: 04000022_005_202412.csv")
print("Describe VALORE:")
print(df['VALORE'].describe())

print("\n-----------------------------------")

df = pd.read_csv("./data/sensor_measurements_35pc.csv", header=0)
print("\nFile name: sensor_measurements_35pc.csv")
print("Describe VALUE:")
print(df['VALUE'].describe())

variations = (df['VALUE'].values[:, None] - df['VALUE'].values) / df['VALUE'].values[:, None] * 100
np.fill_diagonal(variations, np.nan)
print("\nMax variation: ", np.nanmax(variations))


print("\n-----------------------------------")

df = pd.read_csv("./data/sensor_measurements_75pc.csv", header=0)
print("\nFile name: sensor_measurements_75pc.csv")
print("Describe VALUE:")
print(df['VALUE'].describe())

variations = (df['VALUE'].values[:, None] - df['VALUE'].values) / df['VALUE'].values[:, None] * 100
np.fill_diagonal(variations, np.nan)
print("\nMax variation: ", np.nanmax(variations))
