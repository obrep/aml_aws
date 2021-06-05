import pandas as pd
import numpy as np

INPUT_FILE = "data/paysim_gen.csv"
OUTPUT_FILE = "data/transactions.csv"
YEAR = 2021
MONTH = 5

if __name__ == "__main__":
    df = pd.read_csv(INPUT_FILE)
    # Create timestamp based on step + random minute/second

    df["timestamp"] = df.step.map(
        lambda x: pd.Timestamp(
            year=YEAR,
            month=MONTH,
            day=x // 24 + 1,  # Can't have 0th day
            hour=x % 24,
            minute=np.random.randint(0, 59),
            second=np.random.randint(0, 59),
        )
    )

    df.drop(["step"], axis=1, inplace=True)
    columns_to_order = ["timestamp"]
    new_columns = columns_to_order + list(df.columns.drop(columns_to_order))
    df = df[new_columns]
    df.to_csv(OUTPUT_FILE, index=False)
