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

"""
df = pd.read_csv("./data/sensor_measurements_35pc.csv", header=0)
print("\nFile name: sensor_measurements_35pc.csv")
print("Describe VALUE:")
print(df['VALUE'].describe())


variations = (df['VALUE'].values[:, None] - df['VALUE'].values) / df['VALUE'].values * 100
np.fill_diagonal(variations, np.nan)
print("\nMax variation: ", df.std())

print("\n-----------------------------------")

df = pd.read_csv("./data/sensor_measurements_75pc.csv", header=0)
print("\nFile name: sensor_measurements_75pc.csv")
print("Describe VALUE:")
print(df['VALUE'].describe())

variations = (df['VALUE'].values[:, None] - df['VALUE'].values) / df['VALUE'].values * 100
np.fill_diagonal(variations, np.nan)
print("\nMax variation: ", np.nanmax(variations))
"""

original_values = np.array([13, 31, 16, 43, 18, 49, 50, 51, 20, 25, 52, 14])


def generate_value(file, new_std):
    print(f"\n{file}:")

    original_mean = np.mean(original_values)
    original_std = np.std(original_values, ddof=1)

    scale = new_std / original_std
    new_values = (original_values - original_mean) * scale + original_mean
    new_values = np.round(new_values).astype(int)

    df = pd.read_csv(file)
    df['VALUE'] = new_values
    df["VALUE"] = np.maximum(df["VALUE"], 0)
    print("New values:", df["VALUE"].values)

    print("New deviation standard:", df["VALUE"].std(ddof=1))
    df.to_csv(file, index=False)


generate_value("./data/sensor_measurements_10ds.csv", 10)
generate_value("./data/sensor_measurements_20ds.csv", 20)
