module warthog.columns;

import warthog.values;

class Column(T)
{
    T[] data;
    string name;

    this(string name, T[] data = [])
    {
        this.name = name;
        this.data = data;
    }

    void append(T value)
    {
        data ~= value;
    }

    T opIndex(size_t idx)
    {
        return data[idx];
    }

    void opIndexAssign(T value, size_t idx)
    {
        data[idx] = value;
    }

    @property size_t length() const
    {
        return data.length;
    }
}

interface IColumn
{
    @property string name() const;
    @property size_t length() const;
    DataValue getValue(size_t idx);
    void setValue(size_t idx, DataValue value);
    string toString(size_t idx);
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

    @property string name() const
    {
        return col.name;
    }

    @property size_t length() const
    {
        return col.length;
    }

    DataValue getValue(size_t idx)
    {
        return DataValue(col[idx]);
    }

    void setValue(size_t idx, DataValue value)
    {
        col[idx] = value.get!T;
    }

    string toString(size_t idx)
    {
        import std.conv;

        return to!string(col[idx]);
    }

    IColumn slice(size_t start, size_t end)
    {
        return new TCol!T(col.name, col.data[start .. end]);
    }

    IColumn copy()
    {
        return new TCol!T(col.name, col.data.dup);
    }

    void append(T value)
    {
        col.append(value);
    }

    T[] getData()
    {
        return col.data;
    }
}
