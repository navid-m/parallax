import pandas as pd
import time

data = {
    "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Alice", "Bob"],
    "age": [25, 30, 35, 40, 28, 25, 32],
    "salary": [50000, 60000, 70000, 80000, 55000, 50000, 65000],
    "department": ["IT", "HR", "IT", "Finance", "IT", "HR", "HR"]
}

df = pd.DataFrame(data)

def benchmark(label, func):
    start = time.perf_counter()
    result = func()
    end = time.perf_counter()
    print(f"{label}: {(end - start)*1000:.3f} ms")
    return result

benchmark("Describe", lambda: df.describe())
benchmark("Value counts", lambda: df["name"].value_counts())
benchmark("FillNA", lambda: df.fillna("Unknown"))
benchmark("Pivot table", lambda: pd.pivot_table(df, values="salary", index="department", columns="name", aggfunc="mean"))
benchmark("Apply", lambda: df.apply(lambda row: sum(1 for val in row if pd.notna(val)), axis=1))
