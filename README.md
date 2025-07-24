# Parallax

Parallax is a powerful pandas/polars-like DataFrames library.

It has good functionality for data analysis, transformation, and time series manipulation.

## Usage

```d
import parallax.dataframes;

auto df = createDataFrame(
    ["name", "age", "salary"],
    ["Alice", "Bob", "Charlie"],
    [25, 30, 35],
    [50000.0, 60000.0, 70000.0]
);

df.show();

auto head = df.head(2);
auto selected = df.select("name", "salary");

auto summary = df.describe();
summary.showPivot();
```

---

## Core Components

| Function             | Description                                     |
| -------------------- | ----------------------------------------------- |
| `create(...)`        | Construct a DataFrame from columns              |
| `opIndex(...)`       | Access column(s) or row(s) via indexing         |
| `opIndexAssign(...)` | Assign values via index                         |
| `rows`               | Number of rows                                  |
| `cols`               | Number of columns                               |
| `columns()`          | Returns list of column names                    |
| `shape()`            | Tuple of (rows, cols)                           |
| `opSlice(...)`       | Slice the DataFrame                             |
| `head(n)`            | Return the first `n` rows                       |
| `tail(n)`            | Return the last `n` rows                        |
| `select(...)`        | Select one or more columns                      |
| `where(predicate)`   | Filter rows by custom condition                 |
| `groupBy(col)`       | Group by one or more columns                    |
| `pivotTable(...)`    | Create a pivot table                            |
| `melt(...)`          | Reshape from wide to long format                |
| `meltAllExcept(...)` | Melt all columns except the specified ones      |
| `meltMultiple(...)`  | Melt multiple sets of columns                   |
| `unmelt(...)`        | Reshape from long to wide format                |
| `stack()`            | Stack columns into a single column              |
| `unstack()`          | Unstack a stacked column back to wide           |
| `crosstab(...)`      | Cross-tabulate two columns                      |
| `rolling(window)`    | Rolling window statistics                       |
| `pctChange()`        | Compute percent change                          |
| `shift(n)`           | Shift rows forward/backward                     |
| `ffill()`            | Forward fill missing values                     |
| `bfill()`            | Backward fill missing values                    |
| `createLags(n)`      | Create lagged versions of columns               |
| `describe()`         | Summary statistics                              |
| `valueCounts(col)`   | Count frequency of values in a column           |
| `fillna(value)`      | Replace missing values                          |
| `drop(cols...)`      | Drop specified columns                          |
| `dropna()`           | Drop rows with missing data                     |
| `rename(old => new)` | Rename columns                                  |
| `apply(fn)`          | Apply a custom function to each row             |
| `show()`             | Print the DataFrame                             |
| `showPivot()`        | Display a pivot table nicely                    |
| `readCsv(path)`      | Read a CSV file into a DataFrame                |
| `toCsv(path)`        | Write a DataFrame to CSV                        |
| `copy()`             | Deep copy the DataFrame                         |
| `merge(other)`       | Merge two DataFrames (default is inner join)    |
| `sortValues(by)`     | Sort by a specific column                       |
| `sum()`              | Sum across numeric columns                      |
| `mean()`             | Mean across numeric columns                     |
| `max()`              | Max across numeric columns                      |
| `min()`              | Min across numeric columns                      |
| `opBinary(...)`      | Binary operations between DataFrames or columns |

---

### 🕒 Time Series & Dates

| Function / Feature         | Description                             |
| -------------------------- | --------------------------------------- |
| `toDatetime(col)`          | Convert string column to datetime       |
| `dt(col)`                  | Return datetime column accessor         |
| `setDatetimeIndex(col)`    | Use datetime column as an index         |
| `resample(freq)`           | Resample time series by frequency       |
| `betweenDates(start, end)` | Filter rows between two datetime values |
| `forYear(year)`            | Filter for a specific year              |
| `forMonth(month)`          | Filter for a specific month             |
| `rollup()`                 | Aggregate over time hierarchy           |

#### Datetime Accessors (`dt(...)`)

| Accessor           | Description                   |
| ------------------ | ----------------------------- |
| `dt_year()`        | Extract year                  |
| `dt_month()`       | Extract month                 |
| `dt_day()`         | Extract day                   |
| `dt_dayofweek()`   | Extract day of the week (0–6) |
| `dt_strftime(fmt)` | Format datetime as string     |
| `dt_floor(freq)`   | Round down to nearest freq    |
| `dt_ceil(freq)`    | Round up to nearest freq      |

---

## Modules

| Module                | Purpose                                 |
| --------------------- | --------------------------------------- |
| `parallax.dataframes` | Core DataFrame class and utilities      |
| `parallax.columns`    | Column types and column management      |
| `parallax.csv`        | CSV reader/writer                       |
| `parallax.values`     | Generic value abstraction (`DataValue`) |
| `parallax.datetime`   | Datetime parsing and time logic         |

---

### CSV I/O

```d
auto df = readCsv("data.csv");
df.toCsv("output.csv");
```
