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

original_values = np.array([19,38,20,37,21,41,41,43,23,27,38,19,21,22,37,36,24,36,36,23,25])
print(np.mean(original_values))


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
