/**
 * Authors: Navid M
 * License: GPL3
 * Description: Contains operations for dataframes.
 */

module parallax.dataframes;

import parallax.columns;
import parallax.values;
import std.typecons;
import std.exception;
import std.range;
import std.traits;
import std.algorithm;
import std.stdio;
import std.conv;
import std.array;
import std.string;
import std.file;
import std.parallelism;
import std.variant;

DataFrame createDataFrame(T...)(string[] names, T data) => DataFrame.create(names, data);

/**
 * A mapping of string to variant.
 *
 * Represents a group of dataframes.
 */
alias GroupedDataFrame = Variant[string];

/**
 * A container for regular numerical and time-series related data.
 */
class DataFrame
{
    private IColumn[] columns_;
    private string[] columnNames_;
    private size_t[string] nameToIndex_;

    /**
     * Construct an empty dataframe.
     */
    this()
    {
    }

    /**
     * Construct a dataframe with the given columns.
     *
     * Params:
     *   cols = An array of columns to add to the dataframe.
     */
    this(IColumn[] cols)
    {
        foreach (col; cols)
        {
            addColumn(col);
        }
    }

    /**
     * Get back a dataframe given some configuration.
     *
     * Params:
     *   names = Names of columns
     *   arrays = Column content
     *
     * Returns: The dataframe
     */
    static DataFrame create(T...)(string[] names, T arrays)
    {
        auto df = new DataFrame();
        foreach (i, arr; arrays)
        {
            static if (isArray!(typeof(arr)))
            {
                alias ElementType = typeof(arr[0]);
                df.addColumn(new TCol!ElementType(names[i], arr));
            }
        }
        return df;
    }

    /**
     * Add a column to this dataframe.
     *
     * Params:
     *   col = Column to add
     */
    private void addColumn(IColumn col)
    {
        columns_ ~= col;
        columnNames_ ~= col.name;
        nameToIndex_[col.name] = cast(int) columns_.length - 1;
    }

    /**
     * Get a column by its name.
     *
     * Params:
     *   colName = The name of the column.
     * Returns: The column.
     */
    IColumn opIndex(string colName)
    {
        if (colName !in nameToIndex_)
        {
            throw new Exception("Column '" ~ colName ~ "' not found");
        }
        return columns_[nameToIndex_[colName]];
    }

    /**
     * Get a data value at a specific row and column name.
     *
     * Params:
     *   row = The row index.
     *   col = The column name.
     * Returns: The data value.
     */
    DataValue opIndex(size_t row, string col) => this[col].getValue(row);

    /**
     * Get a data value at a specific row and column index.
     *
     * Params:
     *   row = The row index.
     *   col = The column index.
     * Returns: The data value.
     */
    DataValue opIndex(size_t row, size_t col) => columns_[col].getValue(row);

    /**
     * Assign a data value at a specific row and column name.
     *
     * Params:
     *   value = The value to assign.
     *   row = The row index.
     *   col = The column name.
     */
    void opIndexAssign(DataValue value, size_t row, string col)
    {
        this[col].setValue(row, value);
    }

    /**
     * Assign a data value at a specific row and column index.
     *
     * Params:
     *   value = The value to assign.
     *   row = The row index.
     *   col = The column index.
     */
    void opIndexAssign(DataValue value, size_t row, size_t col)
    {
        columns_[col].setValue(row, value);
    }

    /**
     * Get the number of rows in the DataFrame.
     *
     * Returns: The number of rows.
     */
    @property size_t rows() const => columns_.length > 0 ? columns_[0].length : 0;

    /**
     * Get the number of columns in the DataFrame.
     *
     * Returns: The number of columns.
     */
    @property size_t cols() const => columns_.length;

    /**
     * Get the column names of the DataFrame.
     *
     * Returns: An array of column names.
     */
    @property string[] columns() const => columnNames_.dup;

    /**
     * Get the shape (rows, columns) of the DataFrame.
     *
     * Returns: A tuple containing the number of rows and columns.
     */
    @property Tuple!(size_t, size_t) shape() const => tuple(rows, cols);

    /**
     * Slice the DataFrame by row range.
     *
     * Params:
     *   rowStart = The starting row index (inclusive).
     *   rowEnd = The ending row index (exclusive).
     * Returns: A new DataFrame containing the sliced rows.
     */
    DataFrame opSlice(size_t rowStart, size_t rowEnd)
    {
        auto newCols = columns_.map!(col => col.slice(rowStart, rowEnd)).array;
        return new DataFrame(newCols);
    }

    /**
     * Get the first `n` rows of the DataFrame.
     *
     * Params:
     *   n = The number of rows to return (default: 5).
     * Returns: A new DataFrame containing the head rows.
     */
    DataFrame head(size_t n = 5)
    {
        size_t end = std.algorithm.min(n, rows);
        return this[0 .. end];
    }

    /**
     * Get the last `n` rows of the DataFrame.
     *
     * Params:
     *   n = The number of rows to return (default: 5).
     * Returns: A new DataFrame containing the tail rows.
     */
    DataFrame tail(size_t n = 5)
    {
        size_t start = rows > n ? rows - n : 0;
        return this[start .. rows];
    }

    /**
     * Select a subset of columns from the DataFrame.
     *
     * Params:
     *   colNames = An array of column names to select.
     * Returns: A new DataFrame containing only the selected columns.
     */
    DataFrame select(string[] colNames...)
    {
        IColumn[] selectedCols;
        foreach (name; colNames)
        {
            selectedCols ~= this[name].copy();
        }
        return new DataFrame(selectedCols);
    }

    /**
     * Filter the DataFrame based on a boolean mask.
     *
     * Params:
     *   mask = A boolean array indicating which rows to keep.
     * Returns: A new DataFrame containing only the filtered rows.
     */
    DataFrame where(bool[] mask)
    {
        enforce(mask.length == rows, "Mask length must match number of rows");
        size_t filteredRowCount = 0;
        foreach (include; mask)
        {
            if (include)
                filteredRowCount++;
        }
        if (filteredRowCount == 0)
        {
            IColumn[] emptyCols;
            foreach (col; columns_)
            {
                emptyCols ~= col.createEmpty();
            }
            return new DataFrame(emptyCols);
        }
        IColumn[] filteredCols;
        foreach (col; columns_)
        {
            filteredCols ~= col.filter(mask);
        }
        return new DataFrame(filteredCols);
    }

    /**
     * Group the DataFrame by a specified column.
     *
     * Params:
     *   colName = The name of the column to group by.
     * Returns: A GroupedDataFrame where keys are unique values from the grouping column
     * and values are DataFrames containing the grouped rows.
     */
    GroupedDataFrame groupBy(string colName)
    {
        import std.algorithm : uniq, sort;
        import std.array : array;
        import std.variant : Variant;
        import std.conv : to;

        auto groupColIndex = -1;
        foreach (i, col; columns_)
        {
            if (col.name == colName)
            {
                groupColIndex = cast(int) i;
                break;
            }
        }
        enforce(groupColIndex != -1, "Group column not found: " ~ colName);

        auto groupCol = columns_[groupColIndex];
        Variant[] keys;
        foreach (i; 0 .. groupCol.length)
        {
            keys ~= groupCol.getValue(i);
        }

        auto uniqueKeys = keys.sort.uniq.array;
        GroupedDataFrame result;

        foreach (key; uniqueKeys)
        {
            bool[] mask;
            foreach (i; 0 .. groupCol.length)
            {
                mask ~= (groupCol.getValue(i) == key);
            }
            result[to!string(key)] = this.where(mask);
        }

        return result;
    }

    /**
     * Convert a specified column to a DateTimeColumn.
     *
     * Params:
     *   colName = The name of the column to convert.
     *   format = Optional format string for parsing dates.
     * Returns: A new DataFrame with the specified column converted to DateTimeColumn.
     */
    DataFrame toDatetime(string colName, string format = "")
    {
        enforce(colName in nameToIndex_, "Column not found: " ~ colName);

        auto col = this[colName];
        auto newData = new ParallaxDateTime[](rows);

        foreach (i; 0 .. rows)
        {
            auto strVal = col.toString(i);
            newData[i] = parseDateTime(strVal, format);
        }

        auto newCols = columns_.dup;
        auto dateTimeCol = new DateTimeColumn(colName, newData);
        newCols[nameToIndex_[colName]] = cast(IColumn) dateTimeCol;

        return new DataFrame(newCols);
    }

    import parallax.datetime;

    /**
     * Accesses a DateTimeColumn from the DataFrame.
     *
     * Params:
     *   colName = The name of the column to access.
     * Returns: The DateTimeColumn.
     */
    DateTimeColumn dt(string colName)
    {
        auto col = cast(DateTimeColumn) this[colName];
        enforce(col !is null, "Column is not a datetime column: " ~ colName);
        return col;
    }

    /**
    * Set a datetime index (for time series operations)
    */
    DataFrame setDatetimeIndex(string colName)
    {
        auto dtCol = cast(DateTimeColumn) this[colName];
        enforce(dtCol !is null, "Column must be datetime type: " ~ colName);
        auto newCols = columns_.filter!(col => col.name != colName).array;
        auto result = new DataFrame(newCols);
        return result;
    }

    /**
    * Resample datetime data by frequency
    */
    DataFrame resample(string colName, string freq, string aggFunc = "mean")
    {
        auto dtCol = cast(DateTimeColumn) this[colName];
        enforce(dtCol !is null, "Column must be datetime type: " ~ colName);

        auto dtData = dtCol.getData();
        if (dtData.length == 0)
            return new DataFrame();

        auto minDate = dtData[0];
        auto maxDate = dtData[0];
        foreach (dt; dtData)
        {
            if (dt < minDate)
                minDate = dt;
            if (dt > maxDate)
                maxDate = dt;
        }

        auto periods = splitByFrequency(minDate, maxDate, freq);
        string[] periodNames;
        IColumn[] resultCols;
        auto periodCol = new TCol!string("period");
        foreach (period; periods)
        {
            periodCol.append(period.start.toString());
        }
        resultCols ~= cast(IColumn) periodCol;

        foreach (col; columns_)
        {
            if (col.name == colName)
                continue;

            auto stringCol = cast(TCol!string) col;
            if (stringCol)
                continue;

            auto newCol = new TCol!double(col.name);

            foreach (period; periods)
            {
                double[] values;
                foreach (i; 0 .. dtData.length)
                {
                    if (period.contains(dtData[i]))
                    {
                        auto val = col.getValue(i);
                        if (val.convertsTo!double)
                            values ~= val.get!double;
                        else if (val.convertsTo!int)
                            values ~= cast(double) val.get!int;
                    }
                }

                double result = 0.0;
                if (values.length > 0)
                {
                    switch (aggFunc.toLower)
                    {
                    case "mean":
                        result = values.sum / values.length;
                        break;
                    case "sum":
                        result = values.sum;
                        break;
                    case "min":
                        result = values.minElement;
                        break;
                    case "max":
                        result = values.maxElement;
                        break;
                    case "count":
                        result = cast(double) values.length;
                        break;
                    default:
                        result = values.sum / values.length;
                    }
                }
                newCol.append(result);
            }

            resultCols ~= cast(IColumn) newCol;
        }

        return new DataFrame(resultCols);
    }

    /**
    * Filter data by date range
    */
    DataFrame betweenDates(string colName, ParallaxDateTime start, ParallaxDateTime end)
    {
        auto dtCol = cast(DateTimeColumn) this[colName];
        enforce(dtCol !is null, "Column must be datetime type: " ~ colName);

        auto dtData = dtCol.getData();
        bool[] mask = new bool[](rows);

        foreach (i; 0 .. dtData.length)
        {
            mask[i] = (dtData[i] >= start && dtData[i] <= end);
        }

        return where(mask);
    }

    /**
    * Filter data by date range using string dates
    */
    DataFrame betweenDates(string colName, string startStr, string endStr)
    {
        auto start = parseDateTime(startStr);
        auto end = parseDateTime(endStr);
        return betweenDates(colName, start, end);
    }

    /**
    * Get data for a specific year
    */
    DataFrame forYear(string colName, int year)
    {
        auto start = ParallaxDateTime(year, 1, 1);
        auto end = ParallaxDateTime(year, 12, 31, 23, 59, 59);
        return betweenDates(colName, start, end);
    }

    /**
    * Get data for a specific month
    */
    DataFrame forMonth(string colName, int year, int month)
    {
        import std.datetime;

        auto start = ParallaxDateTime(year, month, 1);
        auto lastDay = month == 12 ?
            ParallaxDateTime(year + 1, 1, 1) + (-1).days
            : ParallaxDateTime(year, month + 1, 1) + (-1)
                .days;
        return betweenDates(colName, start, lastDay);
    }

    DataFrame rollup(string dateCol, string valueCol, string period = "daily", string aggFunc = "sum")
    {
        import std.datetime;

        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);

        auto valCol = this[valueCol];
        auto dtData = dtCol.getData();

        string[ParallaxDateTime] periodMap;
        double[string] aggregatedValues;
        int[string] counts;

        foreach (i; 0 .. dtData.length)
        {
            auto dt = dtData[i];
            string periodKey;

            switch (period.toLower)
            {
            case "daily":
                periodKey = format("%04d-%02d-%02d", dt.year, dt.month, dt.day);
                break;
            case "weekly":
                auto monday = dt.floor("day") + (1 - dt.dayOfWeek).days;
                periodKey = format("%04d-W%02d", monday.year,
                    (monday.dayOfYear - 1) / 7 + 1);
                break;
            case "monthly":
                periodKey = format("%04d-%02d", dt.year, dt.month);
                break;
            case "yearly":
                periodKey = format("%04d", dt.year);
                break;
            default:
                periodKey = format("%04d-%02d-%02d", dt.year, dt.month, dt.day);
            }

            auto val = valCol.getValue(i);
            double numVal = 0.0;
            if (val.convertsTo!double)
                numVal = val.get!double;
            else if (val.convertsTo!int)
                numVal = cast(double) val.get!int;
            else if (val.convertsTo!long)
                numVal = cast(double) val.get!long;

            if (periodKey !in aggregatedValues)
            {
                aggregatedValues[periodKey] = 0.0;
                counts[periodKey] = 0;
            }

            switch (aggFunc.toLower)
            {
            case "sum":
                aggregatedValues[periodKey] += numVal;
                break;
            case "mean":
                aggregatedValues[periodKey] += numVal;
                counts[periodKey]++;
                break;
            case "count":
                aggregatedValues[periodKey] += 1;
                break;
            case "min":
                if (counts[periodKey] == 0 || numVal < aggregatedValues[periodKey])
                    aggregatedValues[periodKey] = numVal;
                break;
            case "max":
                if (counts[periodKey] == 0 || numVal > aggregatedValues[periodKey])
                    aggregatedValues[periodKey] = numVal;
                break;
            default:
                aggregatedValues[periodKey] += numVal;
            }
            counts[periodKey]++;
        }

        if (aggFunc.toLower == "mean")
        {
            foreach (key; aggregatedValues.keys)
            {
                if (counts[key] > 0)
                    aggregatedValues[key] /= counts[key];
            }
        }

        auto periodKeys = aggregatedValues.keys.sort().array;
        auto periodCol = new TCol!string(period);
        auto valueCol2 = new TCol!double(valueCol ~ "_" ~ aggFunc);

        foreach (key; periodKeys)
        {
            periodCol.append(key);
            valueCol2.append(aggregatedValues[key]);
        }

        return new DataFrame([cast(IColumn) periodCol, cast(IColumn) valueCol2]);
    }

    DataFrame melt(string[] id_vars = [], string[] value_vars = [],
        string var_name = "variable", string value_name = "value")
    {
        if (value_vars.length == 0)
        {
            foreach (colName; columnNames_)
            {
                if (!id_vars.canFind(colName))
                {
                    value_vars ~= colName;
                }
            }
        }

        foreach (col; id_vars)
        {
            enforce(col in nameToIndex_, "ID variable column not found: " ~ col);
        }
        foreach (col; value_vars)
        {
            enforce(col in nameToIndex_, "Value variable column not found: " ~ col);
        }

        size_t resultRows = rows * value_vars.length;

        IColumn[] resultCols;

        foreach (idVar; id_vars)
        {
            auto originalCol = this[idVar];
            auto newCol = new TCol!string(idVar);

            foreach (i; 0 .. rows)
            {
                string value = originalCol.toString(i);
                foreach (j; 0 .. value_vars.length)
                {
                    newCol.append(value);
                }
            }

            resultCols ~= cast(IColumn) newCol;
        }

        auto varCol = new TCol!string(var_name);
        foreach (i; 0 .. rows)
        {
            foreach (valueVar; value_vars)
            {
                varCol.append(valueVar);
            }
        }
        resultCols ~= cast(IColumn) varCol;

        auto valueCol = new TCol!string(value_name);
        foreach (i; 0 .. rows)
        {
            foreach (valueVar; value_vars)
            {
                auto originalCol = this[valueVar];
                valueCol.append(originalCol.toString(i));
            }
        }
        resultCols ~= cast(IColumn) valueCol;

        return new DataFrame(resultCols);
    }

    /**
    * Convenience overload for melting with a single id variable
    */
    DataFrame melt(string id_var, string[] value_vars = [],
        string var_name = "variable", string value_name = "value")
    {
        return melt([id_var], value_vars, var_name, value_name);
    }

    /**
    * Convenience overload for melting all columns except one id variable
    */
    DataFrame meltAllExcept(string id_var, string var_name = "variable", string value_name = "value")
    {
        return melt([id_var], [], var_name, value_name);
    }

    /**
    * Wide to long transformation with multiple value columns
    * More advanced melt that can handle multiple value column sets
    */
    DataFrame meltMultiple(string[] id_vars, string[][] value_var_groups,
        string[] var_names, string[] value_names)
    {
        enforce(value_var_groups.length == var_names.length &&
                var_names.length == value_names.length,
            "Number of value variable groups must match number of variable and value names");

        DataFrame result;

        foreach (i, valueVars; value_var_groups)
        {
            auto melted = melt(id_vars, valueVars, var_names[i], value_names[i]);

            if (i == 0)
            {
                result = melted;
            }
            else
            {
                auto newCols = new IColumn[](result.cols + melted.cols - id_vars.length);
                foreach (j, col; result.columns_)
                {
                    newCols[j] = col.copy();
                }
                size_t newColIndex = result.cols;
                foreach (j, col; melted.columns_)
                {
                    if (!id_vars.canFind(col.name))
                    {
                        newCols[newColIndex] = col.copy();
                        newColIndex++;
                    }
                }

                result = new DataFrame(newCols[0 .. newColIndex]);
            }
        }

        return result;
    }

    /**
    * Reverse operation: pivot (unpivot the melted data back to wide format)
    * This complements the melt operation
    */
    DataFrame unmelt(string index_col, string var_col, string value_col)
    {
        auto varColumn = this[var_col];
        bool[string] uniqueVars;
        foreach (i; 0 .. varColumn.length)
        {
            uniqueVars[varColumn.toString(i)] = true;
        }
        auto newColNames = uniqueVars.keys.sort().array;
        auto indexColumn = this[index_col];
        bool[string] uniqueIndices;
        foreach (i; 0 .. indexColumn.length)
        {
            uniqueIndices[indexColumn.toString(i)] = true;
        }
        auto indexValues = uniqueIndices.keys.sort().array;
        IColumn[] resultCols;
        auto newIndexCol = new TCol!string(index_col, indexValues.dup);
        resultCols ~= cast(IColumn) newIndexCol;
        auto valueColumn = this[value_col];
        foreach (varName; newColNames)
        {
            auto newCol = new TCol!string(varName);

            foreach (indexVal; indexValues)
            {
                string foundValue = "";

                foreach (i; 0 .. rows)
                {
                    if (indexColumn.toString(i) == indexVal &&
                        varColumn.toString(i) == varName)
                    {
                        foundValue = valueColumn.toString(i);
                        break;
                    }
                }

                newCol.append(foundValue);
            }

            resultCols ~= cast(IColumn) newCol;
        }

        return new DataFrame(resultCols);
    }

    DataFrame stack(string[] columns_to_stack, string level_name = "level_1")
    {
        if (columns_to_stack.length == 0)
            return this.copy();
        string[] id_columns;
        foreach (colName; columnNames_)
        {
            if (!columns_to_stack.canFind(colName))
            {
                id_columns ~= colName;
            }
        }
        return melt(id_columns, columns_to_stack, level_name, "value");
    }

    /**
    * Unstack operation - reverse of stack
    */
    DataFrame unstack(string level_col, string value_col)
    {
        return unmelt("", level_col, value_col);
    }

    /**
    * Cross-tabulation using melt as a helper
    */
    DataFrame crosstab(
        string index_col,
        string columns_col,
        string values_col = "",
        string aggfunc = "count"
    )
    {
        if (values_col == "")
        {
            return this.pivotTable("", index_col, columns_col, "count");
        }
        else
        {
            return this.pivotTable(values_col, index_col, columns_col, aggfunc);
        }
    }

    /**
    * Calculate rolling window statistics over time
    */
    DataFrame rolling(string dateCol, string valueCol, int window, string aggFunc = "mean")
    {
        import std.math;

        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);
        enforce(window > 0, "Window size must be positive");

        auto valCol = this[valueCol];
        auto resultPeriodCol = new TCol!string(dateCol);
        auto resultValueCol = new TCol!double(valueCol ~ "_rolling_" ~ aggFunc);

        foreach (i; 0 .. rows)
        {
            if (i < window - 1)
            {
                resultPeriodCol.append(dtCol.toString(i));
                resultValueCol.append(double.nan);
                continue;
            }

            double[] windowValues;
            foreach (j; (i - window + 1) .. (i + 1))
            {
                auto val = valCol.getValue(j);
                if (val.convertsTo!double)
                    windowValues ~= val.get!double;
                else if (val.convertsTo!int)
                    windowValues ~= cast(double) val.get!int;
                else if (val.convertsTo!long)
                    windowValues ~= cast(double) val.get!long;
            }

            double result = 0.0;
            if (windowValues.length > 0)
            {
                switch (aggFunc.toLower)
                {
                case "mean":
                    result = windowValues.sum / windowValues.length;
                    break;
                case "sum":
                    result = windowValues.sum;
                    break;
                case "min":
                    result = windowValues.minElement;
                    break;
                case "max":
                    result = windowValues.maxElement;
                    break;
                case "std":
                    auto mean = windowValues.sum / windowValues.length;
                    auto variance = windowValues.map!(x => (x - mean) * (x - mean))
                        .sum / windowValues.length;
                    result = sqrt(variance);
                    break;
                default:
                    result = windowValues.sum / windowValues.length;
                }
            }
            else
            {
                result = double.nan;
            }

            resultPeriodCol.append(dtCol.toString(i));
            resultValueCol.append(result);
        }

        return new DataFrame([
            cast(IColumn) resultPeriodCol, cast(IColumn) resultValueCol
        ]);
    }

    /**
    * Calculate period-over-period changes
    */
    DataFrame pctChange(string dateCol, string valueCol, int periods = 1)
    {
        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);

        auto valCol = this[valueCol];

        auto resultDateCol = new TCol!string(dateCol);
        auto resultChangeCol = new TCol!double(valueCol ~ "_pct_change");

        foreach (i; 0 .. rows)
        {
            resultDateCol.append(dtCol.toString(i));

            if (i < periods)
            {
                resultChangeCol.append(double.nan);
                continue;
            }

            auto currentVal = valCol.getValue(i);
            auto previousVal = valCol.getValue(i - periods);

            double current = 0.0, previous = 0.0;
            bool currentValid = false, previousValid = false;

            if (currentVal.convertsTo!double)
            {
                current = currentVal.get!double;
                currentValid = true;
            }
            else if (currentVal.convertsTo!int)
            {
                current = cast(double) currentVal.get!int;
                currentValid = true;
            }

            if (previousVal.convertsTo!double)
            {
                previous = previousVal.get!double;
                previousValid = true;
            }
            else if (previousVal.convertsTo!int)
            {
                previous = cast(double) previousVal.get!int;
                previousValid = true;
            }

            if (currentValid && previousValid && previous != 0.0)
            {
                double change = (current - previous) / previous;
                resultChangeCol.append(change);
            }
            else
            {
                resultChangeCol.append(double.nan);
            }
        }

        return new DataFrame([
            cast(IColumn) resultDateCol, cast(IColumn) resultChangeCol
        ]);
    }

    /**
    * Shift data by a number of periods
    */
    DataFrame shift(string dateCol, string valueCol, int periods = 1)
    {
        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);

        auto valCol = this[valueCol];
        auto resultDateCol = new TCol!string(dateCol);
        auto resultValueCol = new TCol!string(valueCol ~ "_shifted");

        foreach (i; 0 .. rows)
        {
            resultDateCol.append(dtCol.toString(i));

            int sourceIndex = cast(int) i - periods;
            if (sourceIndex >= 0 && sourceIndex < rows)
            {
                resultValueCol.append(valCol.toString(sourceIndex));
            }
            else
            {
                resultValueCol.append("");
            }
        }

        return new DataFrame([
            cast(IColumn) resultDateCol, cast(IColumn) resultValueCol
        ]);
    }

    /**
    * Forward fill missing values based on time order
    */
    DataFrame ffill(string dateCol, string[] fillCols = [])
    {
        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);

        auto targetCols = fillCols.length > 0 ? fillCols : columns_.map!(col => col.name)
            .filter!(name => name != dateCol)
            .array;

        auto newCols = new IColumn[](cols);

        foreach (i, col; columns_)
        {
            if (targetCols.canFind(col.name))
            {
                auto newCol = new TCol!string(col.name);
                string lastValidValue = "";

                foreach (j; 0 .. col.length)
                {
                    string currentValue = col.toString(j);
                    if (currentValue != "" && currentValue != "null" && currentValue != "NaN")
                    {
                        lastValidValue = currentValue;
                        newCol.append(currentValue);
                    }
                    else
                    {
                        newCol.append(lastValidValue);
                    }
                }
                newCols[i] = cast(IColumn) newCol;
            }
            else
            {
                newCols[i] = col.copy();
            }
        }

        return new DataFrame(newCols);
    }

    /**
     * Backward fill missing values based on time order
     */
    DataFrame bfill(string dateCol, string[] fillCols = [])
    {
        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);

        auto targetCols = fillCols.length > 0 ? fillCols : columns_.map!(col => col.name)
            .filter!(name => name != dateCol)
            .array;

        auto newCols = new IColumn[](cols);

        foreach (i, col; columns_)
        {
            if (targetCols.canFind(col.name))
            {
                auto newCol = new TCol!string(col.name);
                string[] values = new string[](col.length);

                foreach (j; 0 .. col.length)
                    values[j] = col.toString(j);

                string nextValidValue = "";
                foreach_reverse (j; 0 .. values.length)
                {
                    if (values[j] != "" && values[j] != "null" && values[j] != "NaN")
                    {
                        nextValidValue = values[j];
                    }
                    else if (nextValidValue != "")
                    {
                        values[j] = nextValidValue;
                    }
                }

                foreach (val; values)
                {
                    newCol.append(val);
                }
                newCols[i] = cast(IColumn) newCol;
            }
            else
            {
                newCols[i] = col.copy();
            }
        }

        return new DataFrame(newCols);
    }

    /**
     * Create lag features for time series analysis
     */
    DataFrame createLags(string dateCol, string valueCol, int[] lags)
    {
        auto dtCol = cast(DateTimeColumn) this[dateCol];
        enforce(dtCol !is null, "Date column must be datetime type: " ~ dateCol);

        auto valCol = this[valueCol];
        IColumn[] resultCols;

        resultCols ~= dtCol.copy();
        resultCols ~= valCol.copy();

        foreach (lag; lags)
        {
            auto lagCol = new TCol!string(format("%s_lag_%d", valueCol, lag));

            foreach (i; 0 .. rows)
            {
                int sourceIndex = cast(int) i - lag;
                if (sourceIndex >= 0 && sourceIndex < rows)
                {
                    lagCol.append(valCol.toString(sourceIndex));
                }
                else
                {
                    lagCol.append("");
                }
            }

            resultCols ~= cast(IColumn) lagCol;
        }

        return new DataFrame(resultCols);
    }

    DataFrame describe()
    {
        if (rows == 0)
            return new DataFrame();

        string[] statNames = [
            "count", "mean", "std", "min", "25%", "50%", "75%", "max"
        ];
        IColumn[] resultCols;
        auto indexCol = new TCol!string("", statNames.dup);
        resultCols ~= cast(IColumn) indexCol;

        foreach (col; columns_)
        {
            auto stringCol = cast(TCol!string) col;
            if (stringCol)
            {
                double[] numericData;
                bool isNumeric = true;
                foreach (val; stringCol.getData())
                {
                    try
                    {
                        numericData ~= to!double(val.strip);
                    }
                    catch (Exception e)
                    {
                        isNumeric = false;
                        break;
                    }
                }

                auto stats = calculateStats(numericData);
                auto statCol = new TCol!string(col.name, stats);
                resultCols ~= cast(IColumn) statCol;
            }
            else
            {
                double[] values;
                foreach (i; 0 .. col.length)
                {
                    auto val = col.getValue(i);
                    if (val.convertsTo!double)
                        values ~= val.get!double;
                    else if (val.convertsTo!int)
                        values ~= cast(double) val.get!int;
                    else if (val.convertsTo!long)
                        values ~= cast(double) val.get!long;
                }

                if (values.length > 0)
                {
                    auto stats = calculateStats(values);
                    auto statCol = new TCol!string(col.name, stats);
                    resultCols ~= cast(IColumn) statCol;
                }
            }
        }

        return new DataFrame(resultCols);
    }

    private string[] calculateStats(double[] data)
    {
        import std.math : sqrt, isNaN;
        import std.algorithm : sort;

        if (data.length == 0)
            return ["0", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN"];

        auto validData = data.filter!(x => !isNaN(x)).array;
        auto count = validData.length;

        if (count == 0)
            return ["0", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN", "NaN"];

        double sum = validData.sum;
        double mean = sum / count;
        double variance = validData.map!(x => (x - mean) * (x - mean)).sum / (count > 1 ? count - 1
                : 1);
        double std = sqrt(variance);

        validData.sort();

        double min = validData[0];
        double max = validData[$ - 1];

        double q25 = percentile(validData, 0.25);
        double q50 = percentile(validData, 0.50);
        double q75 = percentile(validData, 0.75);

        return [
            to!string(count),
            format("%.6f", mean),
            format("%.6f", std),
            format("%.6f", min),
            format("%.6f", q25),
            format("%.6f", q50),
            format("%.6f", q75),
            format("%.6f", max)
        ];
    }

    private double percentile(double[] sortedData, double p)
    {
        if (sortedData.length == 0)
            return double.nan;

        if (sortedData.length == 1)
            return sortedData[0];

        double index = p * (cast(int) sortedData.length - 1);
        size_t lower = cast(size_t) index;
        size_t upper = lower + 1;

        if (upper >= sortedData.length)
            return sortedData[$ - 1];

        double weight = index - lower;
        return sortedData[lower] * (1 - weight) + sortedData[upper] * weight;
    }

    /**
     * Calculates the counts of unique values in a specified column.
     *
     * Params:
     *   colName = The name of the column to calculate value counts for.
     *   ascending = If true, sort counts in ascending order; otherwise, descending.
     *   dropna = If true, exclude missing values from the counts.
     * Returns: A new dataframe with two columns: one for unique values and one for their counts.
     */
    auto valueCounts(string colName, bool ascending = false, bool dropna = true)
    {
        enforce(colName in nameToIndex_, "Column '" ~ colName ~ "' not found");
        auto col = this[colName];
        int[string] counts;

        foreach (i; 0 .. col.length)
        {
            string val = col.toString(i);

            if (dropna && (val == "" || val == "null" || val == "NaN"))
                continue;

            if (val in counts)
                counts[val]++;
            else
                counts[val] = 1;
        }

        string[] values;
        int[] countValues;

        foreach (key, count; counts)
        {
            values ~= key;
            countValues ~= count;
        }

        auto indices = iota(0, values.length).array;

        if (ascending)
            indices.sort!((a, b) => countValues[a] < countValues[b]);
        else
            indices.sort!((a, b) => countValues[a] > countValues[b]);

        string[] sortedValues;
        int[] sortedCounts;
        foreach (idx; indices)
        {
            sortedValues ~= values[idx];
            sortedCounts ~= countValues[idx];
        }

        auto valueCol = new TCol!string(colName, sortedValues);
        auto countCol = new TCol!int("count", sortedCounts);

        return new DataFrame([cast(IColumn) valueCol, cast(IColumn) countCol]);
    }

    DataFrame fillna(DataValue fillValue, string[] columns = [])
    {
        auto newCols = new IColumn[](cols);
        auto targetCols = columns.length > 0 ? columns : columnNames_;

        foreach (i, col; columns_)
        {
            if (targetCols.canFind(col.name))
            {
                auto stringCol = cast(TCol!string) col;
                if (stringCol)
                {
                    auto newCol = new TCol!string(col.name);
                    string fillStr = fillValue.convertsTo!string ? fillValue.get!string : "0";

                    foreach (val; stringCol.getData())
                    {
                        if (val == "" || val == "null" || val == "NaN")
                            newCol.append(fillStr);
                        else
                            newCol.append(val);
                    }
                    newCols[i] = cast(IColumn) newCol;
                }
                else
                {
                    newCols[i] = col.copy();
                    foreach (j; 0 .. col.length)
                    {
                        auto val = col.getValue(j);
                        string strVal = col.toString(j);
                        if (strVal == "" || strVal == "null" || strVal == "NaN")
                        {
                            newCols[i].setValue(j, fillValue);
                        }
                    }
                }
            }
            else
            {
                newCols[i] = col.copy();
            }
        }

        return new DataFrame(newCols);
    }

    DataFrame pivotTable(string values, string index, string columns,
        string aggfunc = "mean", DataValue fillValue = DataValue(""))
    {
        enforce(values in nameToIndex_, "Values column not found");
        enforce(index in nameToIndex_, "Index column not found");
        enforce(columns in nameToIndex_, "Columns column not found");

        auto valCol = this[values];
        auto idxCol = this[index];
        auto colsCol = this[columns];

        bool[string] uniqueIndices, uniqueCols;
        foreach (i; 0 .. rows)
        {
            uniqueIndices[idxCol.toString(i)] = true;
            uniqueCols[colsCol.toString(i)] = true;
        }

        string[] indexValues = uniqueIndices.keys.sort().array;
        string[] columnValues = uniqueCols.keys.sort().array;

        IColumn[] resultCols;
        auto indexCol = new TCol!string(index, indexValues.dup);
        resultCols ~= cast(IColumn) indexCol;

        foreach (colVal; columnValues)
        {
            auto pivotCol = new TCol!string(colVal);

            foreach (idxVal; indexValues)
            {
                double[] matchingValues;
                foreach (i; 0 .. rows)
                {
                    if (idxCol.toString(i) == idxVal && colsCol.toString(i) == colVal)
                    {
                        try
                        {
                            string valStr = valCol.toString(i);
                            if (valStr != "" && valStr != "null" && valStr != "NaN")
                                matchingValues ~= to!double(valStr);
                        }
                        catch (Exception e)
                        {
                        }
                    }
                }

                string result;
                if (matchingValues.length == 0)
                {
                    result = fillValue.convertsTo!string ? fillValue.get!string : "";
                }
                else
                {
                    switch (aggfunc)
                    {
                    case "mean":
                        result = format("%.6f", matchingValues.sum / matchingValues.length);
                        break;
                    case "sum":
                        result = format("%.6f", matchingValues.sum);
                        break;
                    case "count":
                        result = to!string(matchingValues.length);
                        break;
                    case "min":
                        result = format("%.6f", matchingValues.minElement);
                        break;
                    case "max":
                        result = format("%.6f", matchingValues.maxElement);
                        break;
                    default:
                        result = format("%.6f", matchingValues.sum / matchingValues.length);
                    }
                }
                pivotCol.append(result);
            }

            resultCols ~= cast(IColumn) pivotCol;
        }

        return new DataFrame(resultCols);
    }

    import parallax.parquet;

    static DataFrame readParquet(string filename)
    {
        import std.algorithm : max;
        import std.parallelism : parallel, totalCPUs;
        import std.range : chunks, enumerate, iota;
        import std.array : array, appender;

        auto parquetFile = new ParquetFile(filename);
        scope (exit)
            parquetFile.close();

        try
        {
            parquetFile.openForReading();
        }
        catch (ParquetException e)
        {
            throw new Exception("Failed to read Parquet file '" ~ filename ~ "': " ~ e.msg);
        }

        auto schema = parquetFile.getSchema();
        auto rowCount = parquetFile.getRowCount();
        auto colCount = parquetFile.getColumnCount();

        if (rowCount == 0 || colCount == 0)
        {
            return new DataFrame();
        }

        auto columns = new TCol!string[](colCount);
        auto columnNames = parquetFile.getColumnNames();

        foreach (i, colName; columnNames)
        {
            columns[i] = new TCol!string(colName);
            columns[i].reserve(rowCount);
        }

        auto numThreads = totalCPUs;
        auto chunkSize = max(1, rowCount / numThreads);
        auto rowIndices = iota(0, rowCount).chunks(chunkSize).array;
        auto results = new string[][][](rowIndices.length);

        foreach (chunkIdx, chunk; rowIndices)
        {
            results[chunkIdx] = new string[][](colCount);
            foreach (ref colData; results[chunkIdx])
            {
                colData.reserve(chunk.length);
            }
        }

        foreach (item; parallel(rowIndices.enumerate))
        {
            auto chunkIdx = item.index;
            auto chunk = item.value;

            foreach (rowIdx; chunk)
            {
                auto row = parquetFile.readRow(rowIdx);
                foreach (colIdx, value; row)
                {
                    if (colIdx < colCount)
                    {
                        string strValue = convertParquetValueToString(value);
                        results[chunkIdx][colIdx] ~= strValue;
                    }
                }
            }
        }

        foreach (colIdx; 0 .. colCount)
        {
            foreach (chunkResult; results)
            {
                if (colIdx < chunkResult.length && chunkResult[colIdx].length > 0)
                {
                    columns[colIdx].appendRange(chunkResult[colIdx]);
                }
            }
        }

        auto finalCols = new IColumn[](columns.length);
        foreach (i, col; columns)
        {
            finalCols[i] = cast(IColumn) col;
        }

        return new DataFrame(finalCols);
    }

    void toParquet(string filename, bool compress = true)
    {
        import std.algorithm : max;
        import std.parallelism : parallel, totalCPUs;
        import std.range : chunks, enumerate, iota;
        import std.array : array, appender;

        if (rows == 0 || cols == 0)
        {
            throw new Exception("Cannot write empty DataFrame to Parquet");
        }

        auto schema = TableSchema();
        foreach (i, colName; columnNames_)
        {
            auto inferredType = inferParquetType(columns_[i]);
            schema.addColumn(colName, inferredType);
        }

        auto parquetFile = new ParquetFile(filename);
        scope (exit)
            parquetFile.close();

        try
        {
            parquetFile.openForWriting(schema);
        }
        catch (ParquetException e)
        {
            throw new Exception("Failed to create Parquet file '" ~ filename ~ "': " ~ e.msg);
        }

        auto numThreads = totalCPUs;
        auto chunkSize = max(1, rows / numThreads);
        auto rowChunks = iota(0, rows).chunks(chunkSize).array;
        foreach (chunk; rowChunks)
        {
            ParquetRow[] chunkRows;
            chunkRows.reserve(chunk.length);

            foreach (rowIdx; chunk)
            {
                ParquetRow row;
                row.reserve(cols);

                foreach (colIdx; 0 .. cols)
                {
                    string strValue = columns_[colIdx].toString(rowIdx);
                    auto parquetValue = convertStringToParquetValue(strValue, schema
                            .columns[colIdx].type);
                    row ~= parquetValue;
                }

                chunkRows ~= row;
            }

            parquetFile.writeRows(chunkRows);
        }
    }

    private static string convertParquetValueToString(ParquetValue value)
    {
        import std.conv : to;

        if (auto boolPtr = value.peek!bool())
        {
            return (*boolPtr).to!string;
        }
        else if (auto intPtr = value.peek!int())
        {
            return (*intPtr).to!string;
        }
        else if (auto longPtr = value.peek!long())
        {
            return (*longPtr).to!string;
        }
        else if (auto floatPtr = value.peek!float())
        {
            return (*floatPtr).to!string;
        }
        else if (auto doublePtr = value.peek!double())
        {
            return (*doublePtr).to!string;
        }
        else if (auto strPtr = value.peek!string())
        {
            return *strPtr;
        }
        else if (auto bytesPtr = value.peek!(ubyte[])())
        {
            return cast(string)(*bytesPtr);
        }

        return value.toString();
    }

    private static ParquetValue convertStringToParquetValue(string strValue, ParquetType targetType)
    {
        import std.conv : to, ConvException;
        import std.string : strip;

        strValue = strValue.strip();

        try
        {
            final switch (targetType)
            {
            case ParquetType.BOOLEAN:
                bool boolVal = strValue.toLower() == "true" || strValue == "1";
                return ParquetValue(boolVal);

            case ParquetType.INT32:
                return ParquetValue(strValue.to!int);

            case ParquetType.INT64:
                return ParquetValue(strValue.to!long);

            case ParquetType.FLOAT:
                return ParquetValue(strValue.to!float);

            case ParquetType.DOUBLE:
                return ParquetValue(strValue.to!double);

            case ParquetType.BYTE_ARRAY:
            case ParquetType.FIXED_LEN_BYTE_ARRAY:
                return ParquetValue(strValue);
            }
        }
        catch (ConvException e)
        {
            return ParquetValue(strValue);
        }
    }

    private static ParquetType inferParquetType(IColumn column)
    {
        import std.conv : to, ConvException;
        import std.string : strip, toLower;
        import std.algorithm : canFind;

        if (column.length == 0)
        {
            return ParquetType.BYTE_ARRAY;
        }

        size_t sampleSize = std.algorithm.min(100, column.length);
        int intCount = 0;
        int floatCount = 0;
        int boolCount = 0;

        foreach (i; 0 .. sampleSize)
        {
            string value = column.toString(i).strip().toLower();

            if (value.length == 0)
                continue;

            if (value == "true" || value == "false" || value == "0" || value == "1")
            {
                boolCount++;
                continue;
            }

            try
            {
                value.to!long;
                intCount++;
                continue;
            }
            catch (ConvException)
            {
            }

            try
            {
                value.to!double;
                floatCount++;
                continue;
            }
            catch (ConvException)
            {
            }
        }

        if (boolCount > sampleSize * 0.8)
        {
            return ParquetType.BOOLEAN;
        }
        else if (intCount > sampleSize * 0.8)
        {
            return ParquetType.INT64;
        }
        else if ((intCount + floatCount) > sampleSize * 0.8)
        {
            return ParquetType.DOUBLE;
        }
        else
        {
            return ParquetType.BYTE_ARRAY;
        }
    }

    /**
     * Get basic information about the DataFrame in Parquet-compatible format
     */
    void parquetInfo()
    {
        import std.stdio : writeln, writef;

        writeln("DataFrame Parquet Info:");
        writeln("Rows: ", rows);
        writeln("Columns: ", cols);
        writeln("Memory usage (estimated): ", estimateMemoryUsage(), " bytes");
        writeln();
        writeln("Column Information:");
        writef("%-20s %-15s %-10s\n", "Column", "Inferred Type", "Non-null");

        foreach (i, colName; columnNames_)
        {
            auto inferredType = inferParquetType(columns_[i]);
            auto nonNullCount = countNonNullValues(columns_[i]);
            writef("%-20s %-15s %-10s\n", colName, inferredType.to!string, nonNullCount.to!string);
        }
    }

    private size_t estimateMemoryUsage()
    {
        size_t totalSize = 0;
        foreach (col; columns_)
        {
            totalSize += col.length * 10;
        }
        return totalSize;
    }

    private size_t countNonNullValues(IColumn column)
    {
        size_t count = 0;
        foreach (i; 0 .. column.length)
        {
            string value = column.toString(i).strip();
            if (value.length > 0 && value != "null" && value != "NULL" && value != "nan" && value != "NaN")
            {
                count++;
            }
        }
        return count;
    }

    DataFrame apply(T)(T delegate(string[]) func, int axis = 0)
    {
        if (axis == 0)
        {
            auto resultCol = new TCol!string("result");

            foreach (i; 0 .. rows)
            {
                string[] rowData;
                foreach (col; columns_)
                {
                    rowData ~= col.toString(i);
                }
                string result = to!string(func(rowData));
                resultCol.append(result);
            }

            return new DataFrame([cast(IColumn) resultCol]);
        }
        else
        {
            IColumn[] resultCols;

            foreach (col; columns_)
            {
                string[] colData;
                foreach (i; 0 .. col.length)
                {
                    colData ~= col.toString(i);
                }
                string result = to!string(func(colData));
                auto resultCol = new TCol!string(col.name, [result]);
                resultCols ~= cast(IColumn) resultCol;
            }

            return new DataFrame(resultCols);
        }
    }

    private import std.format : format;
    private import std.algorithm : minElement, maxElement, filter, canFind;

    void showPivot(size_t maxRows = 20, size_t maxCols = 10)
    {
        import std.algorithm : min;
        import std.array : array;
        import std.conv : to;
        import std.range : iota;
        import std.stdio : writeln;
        import ark.ui : ArkTerm;

        writeln("DataFrame(", rows, " rows, ", cols, " columns)");

        size_t displayCols = min(maxCols, cols);
        size_t displayRows = min(maxRows, rows);
        string[] headers = columnNames_[0 .. displayCols];
        string[][] tableData;
        foreach (i; 0 .. displayRows)
        {
            string[] row;
            foreach (j; 0 .. displayCols)
                row ~= columns_[j].toString(i);
            tableData ~= row;
        }

        ArkTerm.drawTable(headers, tableData);

        if (rows > maxRows)
            writeln("... (", rows - maxRows, " more rows)");
        if (cols > maxCols)
            writeln("... (", cols - maxCols, " more columns)");
    }

    void show(size_t maxRows = 10, size_t maxCols = 20, size_t maxColWidth = 20, bool showIndex = false)
    {
        import std.algorithm : min;
        import std.conv : to;
        import std.stdio : writeln;
        import ark.ui : ArkTerm;
        import std.array : insertInPlace;

        writeln("DataFrame(", rows, " rows, ", cols, " columns)");

        size_t displayRows = min(maxRows, rows);
        size_t displayColsCount = min(maxCols, this.cols);

        string[] headers;
        if (showIndex)
            headers ~= "";

        IColumn[] displayColsArr;

        if (this.cols > displayColsCount)
        {
            size_t side = displayColsCount / 2;
            displayColsArr ~= columns_[0 .. side];
            displayColsArr ~= columns_[$ - (displayColsCount - side) .. $];
        }
        else
        {
            displayColsArr = columns_.dup;
        }

        foreach (col; displayColsArr)
        {
            if (col.name.length > maxColWidth)
                headers ~= col.name[0 .. maxColWidth - 3] ~ "...";
            else
                headers ~= col.name;
        }

        if (this.cols > displayColsCount)
        {
            size_t side = displayColsCount / 2;
            insertInPlace(headers, side + (showIndex ? 1 : 0), "...");
        }

        string[][] tableData;
        foreach (i; 0 .. displayRows)
        {
            string[] row;
            if (showIndex)
                row ~= to!string(i);

            foreach (col; displayColsArr)
                row ~= col.toString(i);

            if (this.cols > displayColsCount)
            {
                size_t side = displayColsCount / 2;
                insertInPlace(row, side + (showIndex ? 1 : 0), "...");
            }
            tableData ~= row;
        }

        ArkTerm.drawTable(headers, tableData);

        if (rows > maxRows)
            writeln("... (", rows - maxRows, " more rows)");
        if (this.cols > displayColsCount)
            writeln("... (", this.cols - displayColsCount, " more columns)");
    }

    static DataFrame readCsv(string filename, bool hasHeader = true, char delimiter = ',')
    {
        auto content = readText(filename);
        auto lines = content.splitLines();
        if (lines.length == 0)
            return new DataFrame();

        string[] headers;
        size_t dataStart = 0;
        if (hasHeader)
        {
            headers = lines[0].split(delimiter);
            dataStart = 1;
        }
        else
        {
            auto firstLine = lines[0].split(delimiter);
            headers.reserve(firstLine.length);
            foreach (i; 0 .. firstLine.length)
                headers ~= "col" ~ to!string(i);
        }

        auto dataLines = lines[dataStart .. $];
        auto numThreads = totalCPUs;
        auto chunkSize = std.algorithm.max(1, dataLines.length / numThreads);
        auto columns = new TCol!string[](headers.length);
        auto estimatedRowsPerCol = dataLines.length;
        foreach (i, header; headers)
        {
            columns[i] = new TCol!string(header);
            columns[i].reserve(estimatedRowsPerCol);
        }

        auto chunks = dataLines.chunks(chunkSize).array;
        auto results = new string[][][](chunks.length);

        foreach (item; parallel(chunks.enumerate))
        {
            auto chunkIdx = item.index;
            auto chunk = item.value;
            results[chunkIdx] = new string[][](headers.length);
            foreach (ref colData; results[chunkIdx])
            {
                colData.reserve(chunk.length);
            }

            foreach (line; chunk)
            {
                auto fields = line.split(delimiter);
                auto fieldCount = std.algorithm.min(fields.length, headers.length);
                foreach (i; 0 .. fieldCount)
                    results[chunkIdx][i] ~= fields[i].strip;
            }
        }

        foreach (colIdx; 0 .. headers.length)
        {
            foreach (chunkResult; results)
            {
                if (colIdx < chunkResult.length && chunkResult[colIdx].length > 0)
                {
                    columns[colIdx].appendRange(chunkResult[colIdx]);
                }
            }
        }

        auto finalCols = new IColumn[](columns.length);
        foreach (i, col; columns)
        {
            finalCols[i] = cast(IColumn) col;
        }

        return new DataFrame(finalCols);
    }

    void toCsv(string filename, bool writeHeader = true, char delimiter = ',')
    {
        auto file = File(filename, "w");

        if (writeHeader)
        {
            file.write(columnNames_.join(delimiter));
            file.writeln();
        }

        auto numThreads = totalCPUs;
        auto chunkSize = std.algorithm.max(1, rows / numThreads);
        auto chunks = iota(0, rows).chunks(chunkSize).array;
        auto buffers = new string[](chunks.length);

        foreach (item; parallel(chunks.enumerate))
        {
            auto chunkIdx = item.index;
            auto chunk = item.value;
            auto buffer = appender!string();

            foreach (rowIdx; chunk)
            {
                string[] rowData;
                foreach (col; columns_)
                {
                    rowData ~= col.toString(rowIdx);
                }
                buffer.put(rowData.join(delimiter));
                buffer.put('\n');
            }

            buffers[chunkIdx] = buffer.data;
        }

        foreach (buffer; buffers)
        {
            file.write(buffer);
        }

        file.close();
    }

    /**
     * Performs a binary operation (e.g., addition, subtraction) between two dataframes.
     *
     * Params:
     *   op = The operation to perform (e.g., "+", "-").
     *   other = The other dataframe to operate with.
     * Returns: A new dataframe resulting from the binary operation.
     */
    DataFrame opBinary(string op)(DataFrame other) if (op == "+" || op == "-")
    {
        enforce(this.shape == other.shape, "DataFrames must have same shape");

        auto newCols = new IColumn[](cols);
        foreach (i; 0 .. cols)
        {
            newCols[i] = columns_[i].copy();
        }
        return new DataFrame(newCols);
    }

    /**
     * Sorts the dataframe by the values in a specified column.
     *
     * Params:
     *   colName = The name of the column to sort by.
     *   ascending = If true, sort in ascending order; otherwise, descending.
     * Returns: A new dataframe sorted by the specified column.
     */
    DataFrame sortValues(string colName, bool ascending = true)
    {
        enforce(colName in nameToIndex_, "Column '" ~ colName ~ "' not found");

        auto colIdx = nameToIndex_[colName];
        auto indices = iota(0, rows).array;

        indices.sort!((a, b) {
            auto valA = columns_[colIdx].toString(a);
            auto valB = columns_[colIdx].toString(b);

            import std.conv : to;
            import std.string : isNumeric;

            if (valA.isNumeric && valB.isNumeric)
            {
                try
                {
                    auto numA = to!double(valA);
                    auto numB = to!double(valB);
                    return ascending ? numA < numB : numA > numB;
                }
                catch (Exception e)
                {
                }
            }

            return ascending ? valA < valB : valA > valB;
        });

        IColumn[] reorderedCols;
        foreach (col; columns_)
        {
            reorderedCols ~= col.reorder(indices);
        }

        return new DataFrame(reorderedCols);
    }

    /**
     * Calculates the sum of numerical columns in the dataframe.
     *
     * Returns: A new dataframe with a single row containing the sums of numerical columns.
     */
    DataFrame sum()
    {
        if (rows == 0)
            return new DataFrame();

        IColumn[] resultCols;
        foreach (col; columns_)
        {
            try
            {
                auto stringCol = cast(TCol!string) col;
                if (stringCol)
                {
                    continue;
                }

                double total = 0.0;
                foreach (i; 0 .. col.length)
                {
                    auto val = col.getValue(i);
                    if (val.convertsTo!double)
                    {
                        total += val.get!double;
                    }
                    else if (val.convertsTo!int)
                    {
                        total += val.get!int;
                    }
                    else if (val.convertsTo!long)
                    {
                        total += val.get!long;
                    }
                }
                resultCols ~= new TCol!double(col.name, [total]);
            }
            catch (Exception e)
            {
                continue;
            }
        }
        return new DataFrame(resultCols);
    }

    /**
     * Calculates the mean of numerical columns in the dataframe.
     *
     * Returns: A new dataframe with a single row containing the means of numerical columns.
     */
    DataFrame mean()
    {
        if (rows == 0)
            return new DataFrame();

        IColumn[] resultCols;
        foreach (col; columns_)
        {
            try
            {
                auto stringCol = cast(TCol!string) col;
                if (stringCol)
                    continue;

                double total = 0.0;
                size_t count = 0;

                foreach (i; 0 .. col.length)
                {
                    auto val = col.getValue(i);
                    if (val.convertsTo!double)
                    {
                        total += val.get!double;
                        count++;
                    }
                    else if (val.convertsTo!int)
                    {
                        total += val.get!int;
                        count++;
                    }
                    else if (val.convertsTo!long)
                    {
                        total += val.get!long;
                        count++;
                    }
                }

                if (count > 0)
                {
                    resultCols ~= new TCol!double(col.name, [
                        total / count
                    ]);
                }
            }
            catch (Exception e)
            {
                continue;
            }
        }
        return new DataFrame(resultCols);
    }

    /**
     * Calculates the maximum value of numerical columns in the dataframe.
     *
     * Returns: A new dataframe with a single row containing the maximums of numerical columns.
     */
    DataFrame max()
    {
        if (rows == 0)
            return new DataFrame();

        IColumn[] resultCols;
        foreach (col; columns_)
        {
            try
            {
                auto stringCol = cast(TCol!string) col;
                if (stringCol)
                {
                    string maxVal = stringCol.getData()[0];
                    foreach (val; stringCol.getData()[1 .. $])
                    {
                        if (val > maxVal)
                            maxVal = val;
                    }
                    resultCols ~= new TCol!string(col.name, [maxVal]);
                    continue;
                }

                double maxVal = double.min_normal;
                bool hasValue = false;

                foreach (i; 0 .. col.length)
                {
                    auto val = col.getValue(i);
                    double numVal;

                    if (val.convertsTo!double)
                    {
                        numVal = val.get!double;
                    }
                    else if (val.convertsTo!int)
                    {
                        numVal = val.get!int;
                    }
                    else if (val.convertsTo!long)
                    {
                        numVal = val.get!long;
                    }
                    else
                    {
                        continue;
                    }

                    if (!hasValue || numVal > maxVal)
                    {
                        maxVal = numVal;
                        hasValue = true;
                    }
                }

                if (hasValue)
                {
                    resultCols ~= new TCol!double(col.name, [maxVal]);
                }
            }
            catch (Exception e)
            {
                continue;
            }
        }
        return new DataFrame(resultCols);
    }

    /**
     * Calculates the minimum value of numerical columns in the dataframe.
     *
     * Returns: A new dataframe with a single row containing the minimums of numerical columns.
     */
    DataFrame min()
    {
        if (rows == 0)
            return new DataFrame();

        IColumn[] resultCols;
        foreach (col; columns_)
        {
            try
            {
                auto stringCol = cast(TCol!string) col;
                if (stringCol)
                {
                    string minVal = stringCol.getData()[0];
                    foreach (val; stringCol.getData()[1 .. $])
                    {
                        if (val < minVal)
                            minVal = val;
                    }
                    resultCols ~= new TCol!string(col.name, [minVal]);
                    continue;
                }

                double minVal = double.max;
                bool hasValue = false;

                foreach (i; 0 .. col.length)
                {
                    auto val = col.getValue(i);
                    double numVal;

                    if (val.convertsTo!double)
                    {
                        numVal = val.get!double;
                    }
                    else if (val.convertsTo!int)
                    {
                        numVal = val.get!int;
                    }
                    else if (val.convertsTo!long)
                    {
                        numVal = val.get!long;
                    }
                    else
                    {
                        continue;
                    }

                    if (!hasValue || numVal < minVal)
                    {
                        minVal = numVal;
                        hasValue = true;
                    }
                }

                if (hasValue)
                {
                    resultCols ~= new TCol!double(col.name, [minVal]);
                }
            }
            catch (Exception e)
            {
                continue;
            }
        }
        return new DataFrame(resultCols);
    }

    /**
     * Merges this dataframe with another dataframe based on a common column.
     *
     * Params:
     *   other = The other dataframe to merge with.
     *   on = The name of the column to join on.
     *   how = The type of merge to be performed ("inner", "left").
     *
     * Returns: A new dataframe resulting from the merge operation.
     */
    DataFrame merge(DataFrame other, string on, string how = "inner")
    {
        enforce(on in nameToIndex_, "Join column '" ~ on ~ "' not found in left DataFrame");
        enforce(on in other.nameToIndex_, "Join column '" ~ on ~ "' not found in right DataFrame");

        auto leftKeyCol = this[on];
        auto rightKeyCol = other[on];

        size_t[string] rightKeyMap;
        foreach (i; 0 .. other.rows)
        {
            auto key = rightKeyCol.toString(i);
            rightKeyMap[key] = i;
        }

        IColumn[] resultCols;

        foreach (col; columns_)
        {
            auto newCol = new TCol!string(col.name);
            resultCols ~= cast(IColumn) newCol;
        }

        foreach (col; other.columns_)
        {
            if (col.name != on)
            {
                auto newCol = new TCol!string(col.name);
                resultCols ~= cast(IColumn) newCol;
            }
        }

        foreach (leftIdx; 0 .. rows)
        {
            auto leftKey = leftKeyCol.toString(leftIdx);

            if (how == "inner" || how == "left")
            {
                auto rightIdx = leftKey in rightKeyMap;
                if (rightIdx || how == "left")
                {
                    foreach (i, col; columns_)
                    {
                        auto typedCol = cast(TCol!string) resultCols[i];
                        if (typedCol)
                        {
                            typedCol.append(col.toString(leftIdx));
                        }
                    }

                    size_t resultColIdx = columns_.length;
                    foreach (col; other.columns_)
                    {
                        if (col.name != on)
                        {
                            auto typedCol = cast(TCol!string) resultCols[resultColIdx];
                            if (typedCol)
                            {
                                if (rightIdx)
                                {
                                    typedCol.append(col.toString(*rightIdx));
                                }
                                else
                                {
                                    typedCol.append("");
                                }
                            }
                            resultColIdx++;
                        }
                    }
                }
            }
        }

        return new DataFrame(resultCols);
    }

    /**
     * Creates a shallow copy of the dataframe.
     *
     * Returns: A new dataframe with copies of the original columns.
     */
    DataFrame copy()
    {
        auto newCols = columns_.map!(col => col.copy()).array;
        return new DataFrame(newCols);
    }

    /**
     * Drop specified columns from the dataframe.
     *
     * Params:
     *   colNames = An array of column names to drop.
     * Returns: A new dataframe without the specified columns.
     */
    DataFrame drop(string[] colNames...)
    {
        IColumn[] keepCols;
        foreach (col; columns_)
        {
            if (!colNames.canFind(col.name))
            {
                keepCols ~= col.copy();
            }
        }
        return new DataFrame(keepCols);
    }

    /**
     * Drop rows from the dataframe that contain any missing or null values.
     *
     * Returns: A new dataframe with rows containing missing values removed.
     */
    DataFrame dropna()
    {
        if (rows == 0)
            return new DataFrame();

        bool[] keepRows = new bool[rows];
        keepRows[] = true;

        foreach (rowIdx; 0 .. rows)
        {
            foreach (col; columns_)
            {
                auto val = col.toString(rowIdx);
                if (val == "" || val == "null" || val == "NaN")
                {
                    keepRows[rowIdx] = false;
                    break;
                }
            }
        }

        IColumn[] newCols;
        foreach (col; columns_)
        {
            auto stringCol = cast(TCol!string) col;
            if (stringCol)
            {
                auto newCol = new TCol!string(col.name);
                foreach (i; 0 .. rows)
                {
                    if (keepRows[i])
                    {
                        newCol.append(stringCol.getData()[i]);
                    }
                }
                newCols ~= cast(IColumn) newCol;
            }
            else
            {
                auto newCol = new TCol!string(col.name);
                foreach (i; 0 .. rows)
                {
                    if (keepRows[i])
                    {
                        newCol.append(col.toString(i));
                    }
                }
                newCols ~= cast(IColumn) newCol;
            }
        }

        return new DataFrame(newCols);
    }

    /**
     * Rename columns in the dataframe.
     *
     * Params:
     *   mapping = A string-to-string associative array where keys are old column names and values are new column names.
     * Returns: A new dataframe with renamed columns.
     */
    DataFrame rename(string[string] mapping)
    {
        auto newCols = new IColumn[](cols);
        foreach (i, col; columns_)
        {
            if (col.name in mapping)
            {
                newCols[i] = col.copyWithName(mapping[col.name]);
            }
            else
            {
                newCols[i] = col.copy();
            }
        }
        return new DataFrame(newCols);
    }
}
