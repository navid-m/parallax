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
    @property string name() const;
    @property size_t length() const;
    DataValue getValue(size_t idx);
    void setValue(size_t idx, DataValue value);
    string toString(size_t idx) const;
    IColumn slice(size_t start, size_t end);
    IColumn copy();
}

/** 
 * A typed column.
 */
class TCol(T) : IColumn
{
    private Column!T col;

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

    void append(T value)
    {
        col.append(value);
    }

    T[] getData()
    {
        return col.data;
    }
}
