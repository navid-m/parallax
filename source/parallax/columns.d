/**
 * Authors: Navid M
 * License: GPL3
 * Description: Contains operations for columnar operations.
 */

module parallax.columns;

import parallax.values;

/** 
 * Some column in a dataframe.
 */
class Column(T)
{
    T[] data;
    string name;

    this(string name, T[] data = [])
    {
        this.name = name;
        this.data = data;
    }

    /** 
     * Append a value to a column.
     *
     * Params:
     *   value = The value to add, generically typed
     */
    void append(T value)
    {
        data ~= value;
    }

    /** 
     * Get back some value by index.
     *
     * Params:
     *   idx = The index
     * Returns: The corresponding value
     */
    T opIndex(size_t idx) const => data[idx];

    /** 
     * Assign by index.
     *
     * Params:
     *   value = Value to assign
     *   idx = The corresponding index of the value to replace
     */
    void opIndexAssign(T value, size_t idx)
    {
        data[idx] = value;
    }

    /** 
     * Get the data length of the column.
     *
     * Returns: Length of the data
     */
    @property size_t length() const => data.length;
}

interface IColumn
{
    /**
     * Get the name of the column.
     *
     * Returns: Name of the column.
     */
    @property string name() const;

    /**
     * Get the length of the column.
     *
     * Returns: Length of the column.
     */
    @property size_t length() const;

    /**
     * Get a data value at a specific index.
     *
     * Params:
     *   idx = The index.
     * Returns: The data value.
     */
    DataValue getValue(size_t idx);

    /**
     * Set a data value at a specific index.
     *
     * Params:
     *   idx = The index.
     *   value = The data value to set.
     */
    void setValue(size_t idx, DataValue value);

    /**
     * Convert a value at a specific index to its string representation.
     *
     * Params:
     *   idx = The index.
     * Returns: The string representation of the value.
     */
    string toString(size_t idx) const;

    /**
     * Slice the column.
     *
     * Params:
     *   start = Start index.
     *   end = End index.
     * Returns: A new column containing the sliced data.
     */
    IColumn slice(size_t start, size_t end);

    /**
     * Create a copy of the column.
     *
     * Returns: A new column that is a copy of the current one.
     */
    IColumn copy();

    /**
     * Filter the column based on a boolean mask.
     *
     * Params:
     *   mask = A boolean array indicating which rows to keep.
     * Returns: A new column containing only the filtered rows.
     */
    IColumn filter(bool[] mask);

    /**
     * Create an empty column with the same type and name.
     *
     * Returns: An empty column.
     */
    IColumn createEmpty();

    /**
     * Reorder the column based on a new set of indices.
     *
     * Params:
     *   indices = An array of new indices.
     * Returns: A new column with reordered data.
     */
    IColumn reorder(size_t[] indices);

    /**
     * Create a copy of the column with a new name.
     *
     * Params:
     *   newName = The new name for the column.
     * Returns: A new column with the specified name and copied data.
     */
    IColumn copyWithName(string newName);
}

/** 
 * A typed column.
 */
class TCol(T) : IColumn
{
    import std.exception;

    private Column!T col;

    /** 
     * Construct the typed column given a name and some data.
     *
     * Params:
     *   name = The name of the typed column
     *   data = The data
     */
    this(string name, T[] data = [])
    {
        col = new Column!T(name, data);
    }

    /** 
     * Get the name of the typed column.
     *
     * Returns: Name of the typed column
     */
    @property string name() const => col.name;

    /** 
     * Get the length of the column
     *
     * Returns: Column length
     */
    @property size_t length() const => col.length;

    /** 
     * Get some value by index from the typed column.
     *
     * Params:
     *   idx = The index
     * Returns: The corresponding data value
     */
    DataValue getValue(size_t idx)
    {
        return DataValue(col[idx]);
    }

    /**
     * Reserve capacity for the column data.
     *
     * Params:
     *   capacity = The number of elements to reserve space for
     */
    void reserve(size_t capacity)
    {
        col.data.reserve(capacity);
    }

    /**
     * Append multiple values to the column at once.
     *
     * Params:
     *   values = The array of values to append
     */
    void appendRange(T[] values)
    {
        col.data ~= values;
    }

    /** 
     * Set some value by index.
     *
     * Params:
     *   idx = The index
     *   value = The value to assign
     */
    void setValue(size_t idx, DataValue value)
    {
        col[idx] = value.get!T;
    }

    /** 
     * Stringer function for a cell (by index) of the column.
     *
     * Returns: The string representation of the typed column cell.
     */
    string toString(size_t idx) const
    {
        import std.conv;

        return to!string(col[idx]);
    }

    IColumn slice(size_t start, size_t end) => new TCol!T(col.name, col.data[start .. end]);
    IColumn copy() => new TCol!T(col.name, col.data.dup);

    /** 
     * Append some value to the column.
     *
     * Params:
     *   value = The value to append
     */
    void append(T value)
    {
        col.append(value);
    }

    /** 
     * Get back data from the column as a list
     *
     * Returns: The column data
     */
    T[] getData()
    {
        return col.data;
    }

    /** 
     * Create an empty column.
     *
     * Returns: The empty column 
     */
    IColumn createEmpty()
    {
        return new TCol!T(col.name, []);
    }

    IColumn copyWithName(string newName)
    {
        return new TCol!T(newName, col.data.dup);
    }

    IColumn reorder(size_t[] indices)
    {
        T[] reorderedData;
        reorderedData.reserve(indices.length);

        foreach (idx; indices)
        {
            reorderedData ~= col.data[idx];
        }

        return new TCol!T(col.name, reorderedData);
    }

    IColumn filter(bool[] mask)
    {
        enforce(mask.length == col.length, "Mask length must match column length");
        T[] filteredData;
        size_t trueCount = 0;
        foreach (val; mask)
        {
            if (val)
                trueCount++;
        }
        filteredData.reserve(trueCount);

        foreach (i, include; mask)
        {
            if (include)
            {
                filteredData ~= col.data[i];
            }
        }

        return new TCol!T(col.name, filteredData);
    }
}
