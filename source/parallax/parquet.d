module parallax.parquet;

import std.stdio;
import std.file;
import std.exception;
import std.conv;
import std.variant;
import std.typecons;
import std.string;
import std.algorithm;
import std.bitmanip;
import std.range;
import std.array;

enum ParquetType
{
    BOOLEAN,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY
}

enum ParquetEncoding
{
    PLAIN = 0,
    DICTIONARY = 2,
    RLE = 3,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7
}

enum ParquetCompression
{
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3
}

struct ColumnSchema
{
    string name;
    ParquetType type;
    bool required = true;
    int maxDefinitionLevel = 0;
    int maxRepetitionLevel = 0;
    ParquetEncoding encoding = ParquetEncoding.PLAIN;
}

struct TableSchema
{
    ColumnSchema[] columns;

    void addColumn(string name, ParquetType type, bool required = true)
    {
        columns ~= ColumnSchema(name, type, required);
    }
}

struct ColumnMetaData
{
    ParquetType type;
    ParquetEncoding encoding;
    string[] path;
    ParquetCompression compression;
    long numValues;
    long totalUncompressedSize;
    long totalCompressedSize;
    long dataPageOffset;
    long dictionaryPageOffset;
}

struct ColumnChunk
{
    ColumnMetaData metaData;
    long fileOffset;
}

struct RowGroup
{
    ColumnChunk[] columns;
    long totalByteSize;
    long numRows;
}

struct FileMetaData
{
    int version_;
    TableSchema schema;
    long numRows;
    RowGroup[] rowGroups;
    string createdBy;
}

alias ParquetValue = Variant;
alias ParquetRow = ParquetValue[];

class ParquetException : Exception
{
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
    }
}

class ParquetReadException : ParquetException
{
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super("Read error: " ~ msg, file, line);
    }
}

class ParquetWriteException : ParquetException
{
    this(string msg, string file = __FILE__, size_t line = __LINE__)
    {
        super("Write error: " ~ msg, file, line);
    }
}

enum PARQUET_MAGIC = "PAR1";
enum PARQUET_VERSION = 1;

struct DataPage
{
    ubyte[] data;
    long numValues;
    ParquetEncoding encoding;
    long uncompressedSize;
    long compressedSize;
}

class ParquetFile
{
    private
    {
        string filename;
        FileMetaData metadata;
        ParquetRow[] data;
        bool isOpen = false;
        bool isWriteMode = false;
        File file;
    }

    this(string filename)
    {
        this.filename = filename;
    }

    void openForReading()
    {
        if (isOpen)
        {
            throw new ParquetException("File is already open");
        }

        if (!exists(filename))
        {
            throw new ParquetReadException("File does not exist: " ~ filename);
        }

        try
        {
            file = File(filename, "rb");
            readParquetFile();
            isOpen = true;
            isWriteMode = false;
        }
        catch (Exception e)
        {
            throw new ParquetReadException("Failed to open file: " ~ e.msg);
        }
    }

    void openForWriting(TableSchema schema)
    {
        if (isOpen)
        {
            throw new ParquetException("File is already open");
        }

        metadata.schema = schema;
        metadata.version_ = PARQUET_VERSION;
        metadata.numRows = 0;
        metadata.createdBy = "D Parquet Library v1.0";
        this.data = [];

        try
        {
            file = File(filename, "wb");
            isOpen = true;
            isWriteMode = true;
        }
        catch (Exception e)
        {
            throw new ParquetWriteException("Failed to create file: " ~ e.msg);
        }
    }

    void close()
    {
        if (!isOpen)
            return;

        if (isWriteMode)
        {
            try
            {
                writeParquetFile();
            }
            catch (Exception e)
            {
                throw new ParquetWriteException("Failed to write file: " ~ e.msg);
            }
        }

        if (file.isOpen)
        {
            file.close();
        }

        isOpen = false;
        isWriteMode = false;
        data = [];
    }

    TableSchema getSchema()
    {
        if (!isOpen)
        {
            throw new ParquetException("File is not open");
        }
        return metadata.schema;
    }

    ParquetRow[] readAll()
    {
        if (!isOpen || isWriteMode)
        {
            throw new ParquetReadException("File not open for reading");
        }
        return data.dup;
    }

    ParquetRow[] read(size_t maxRows)
    {
        if (!isOpen || isWriteMode)
        {
            throw new ParquetReadException("File not open for reading");
        }

        size_t limit = maxRows > data.length ? data.length : maxRows;
        return data[0 .. limit].dup;
    }

    ParquetRow readRow(size_t index)
    {
        if (!isOpen || isWriteMode)
        {
            throw new ParquetReadException("File not open for reading");
        }

        if (index >= data.length)
        {
            throw new ParquetReadException("Row index out of bounds");
        }

        return data[index].dup;
    }

    void writeRow(ParquetRow row)
    {
        if (!isOpen || !isWriteMode)
        {
            throw new ParquetWriteException("File not open for writing");
        }

        if (row.length != metadata.schema.columns.length)
        {
            throw new ParquetWriteException("Row column count doesn't match schema");
        }

        validateRow(row);
        data ~= row.dup;
        metadata.numRows++;
    }

    void writeRows(ParquetRow[] rows)
    {
        foreach (row; rows)
        {
            writeRow(row);
        }
    }

    size_t getRowCount()
    {
        if (!isOpen)
        {
            throw new ParquetException("File is not open");
        }
        return data.length;
    }

    size_t getColumnCount()
    {
        if (!isOpen)
        {
            throw new ParquetException("File is not open");
        }
        return metadata.schema.columns.length;
    }

    string[] getColumnNames()
    {
        if (!isOpen)
        {
            throw new ParquetException("File is not open");
        }

        string[] names;
        foreach (col; metadata.schema.columns)
        {
            names ~= col.name;
        }
        return names;
    }

    ParquetValue[] getColumn(string columnName)
    {
        if (!isOpen || isWriteMode)
        {
            throw new ParquetReadException("File not open for reading");
        }

        size_t colIndex = size_t.max;
        foreach (i, col; metadata.schema.columns)
        {
            if (col.name == columnName)
            {
                colIndex = i;
                break;
            }
        }

        if (colIndex == size_t.max)
        {
            throw new ParquetReadException("Column not found: " ~ columnName);
        }

        ParquetValue[] columnData;
        foreach (row; data)
        {
            if (colIndex < row.length)
            {
                columnData ~= row[colIndex];
            }
        }

        return columnData;
    }

private:

    void readParquetFile()
    {
        ubyte[4] startMagic;
        file.rawRead(startMagic);
        if (cast(string) startMagic != PARQUET_MAGIC)
        {
            throw new ParquetReadException("Invalid Parquet file: missing magic number");
        }

        file.seek(0, SEEK_END);
        long fileSize = file.tell();

        if (fileSize < 12)
        {
            throw new ParquetReadException("File too small to be valid Parquet");
        }

        file.seek(fileSize - 8);
        ubyte[4] footerLengthBytes;
        file.rawRead(footerLengthBytes);
        uint footerLength = littleEndianToNative!uint(footerLengthBytes[0 .. 4]);

        ubyte[4] endMagic;
        file.rawRead(endMagic);
        if (cast(string) endMagic != PARQUET_MAGIC)
        {
            throw new ParquetReadException("Invalid Parquet file: missing end magic number");
        }

        long footerStart = fileSize - 8 - footerLength;
        if (footerStart < 4)
        {
            throw new ParquetReadException("Invalid footer position");
        }

        file.seek(footerStart);
        ubyte[] footerData = new ubyte[footerLength];
        file.rawRead(footerData);
        parseFooter(footerData);
        readRowGroups();
    }

    void parseFooter(ubyte[] footerData)
    {
        parseSimplifiedThriftFooter(footerData);
    }

    void parseSimplifiedThriftFooter(ubyte[] footerData)
    {
        size_t offset = 0;

        metadata.version_ = PARQUET_VERSION;
        metadata.createdBy = "Unknown";
        metadata.numRows = 0;
        metadata.schema = TableSchema();
        metadata.rowGroups = [];

        if (footerData.length < 16)
        {
            throw new ParquetReadException("Footer too small");
        }

        try
        {
            ubyte[4] versionBytes = footerData[0 .. 4];
            metadata.version_ = littleEndianToNative!int(versionBytes);
            offset += 4;

            ubyte[4] numColBytes = footerData[offset .. offset + 4];
            uint numColumns = littleEndianToNative!uint(numColBytes);
            offset += 4;

            for (uint i = 0; i < numColumns && offset + 8 < footerData.length; i++)
            {
                ubyte[4] nameLenBytes = footerData[offset .. offset + 4];
                uint nameLength = littleEndianToNative!uint(nameLenBytes);
                offset += 4;

                if (offset + nameLength + 4 > footerData.length)
                    break;

                string columnName = cast(string) footerData[offset .. offset + nameLength];
                offset += nameLength;

                ubyte[4] typeBytes = footerData[offset .. offset + 4];
                uint typeValue = littleEndianToNative!uint(typeBytes);
                offset += 4;

                ParquetType columnType = cast(ParquetType)(typeValue % 7);
                metadata.schema.addColumn(columnName, columnType);
            }

            if (offset + 8 <= footerData.length)
            {
                ubyte[8] numRowsBytes = footerData[offset .. offset + 8];
                metadata.numRows = littleEndianToNative!long(numRowsBytes);
                offset += 8;
            }

            if (offset + 4 <= footerData.length)
            {
                ubyte[4] numRgBytes = footerData[offset .. offset + 4];
                uint numRowGroups = littleEndianToNative!uint(numRgBytes);
                offset += 4;

                for (uint rg = 0; rg < numRowGroups && offset + 16 < footerData.length;
                    rg++)
                {
                    RowGroup rowGroup;
                    ubyte[8] rgRowsBytes = footerData[offset .. offset + 8];
                    rowGroup.numRows = littleEndianToNative!long(rgRowsBytes);
                    offset += 8;

                    ubyte[8] rgSizeBytes = footerData[offset .. offset + 8];
                    rowGroup.totalByteSize = littleEndianToNative!long(rgSizeBytes);
                    offset += 8;

                    uint numColChunks = cast(uint) metadata.schema.columns.length;
                    rowGroup.columns = new ColumnChunk[numColChunks];

                    for (uint cc = 0; cc < numColChunks && offset + 24 < footerData.length;
                        cc++)
                    {
                        ColumnChunk chunk;
                        ubyte[8] offsetBytes = footerData[offset .. offset + 8];
                        chunk.fileOffset = littleEndianToNative!long(offsetBytes);
                        offset += 8;

                        chunk.metaData.type = metadata.schema.columns[cc].type;
                        chunk.metaData.encoding = ParquetEncoding.PLAIN;
                        chunk.metaData.compression = ParquetCompression.UNCOMPRESSED;

                        ubyte[8] numValBytes = footerData[offset .. offset + 8];
                        chunk.metaData.numValues = littleEndianToNative!long(numValBytes);
                        offset += 8;

                        ubyte[8] uncompSizeBytes = footerData[offset .. offset + 8];
                        chunk.metaData.totalUncompressedSize = littleEndianToNative!long(
                            uncompSizeBytes);
                        offset += 8;

                        chunk.metaData.totalCompressedSize = chunk.metaData.totalUncompressedSize;
                        chunk.metaData.dataPageOffset = chunk.fileOffset;

                        rowGroup.columns[cc] = chunk;
                    }

                    metadata.rowGroups ~= rowGroup;
                }
            }
        }
        catch (Exception e)
        {
            throw new ParquetReadException("Failed to parse footer: " ~ e.msg);
        }
    }

    void readRowGroups()
    {
        data = [];

        foreach (rowGroup; metadata.rowGroups)
        {
            readRowGroup(rowGroup);
        }
    }

    void readRowGroup(RowGroup rowGroup)
    {
        ParquetValue[][] columnData = new ParquetValue[][metadata.schema.columns.length];

        foreach (i, columnChunk; rowGroup.columns)
        {
            if (i >= metadata.schema.columns.length)
                break;

            try
            {
                columnData[i] = readColumnChunk(columnChunk);
            }
            catch (Exception e)
            {
                columnData[i] = new ParquetValue[rowGroup.numRows];
                foreach (ref val; columnData[i])
                {
                    val = getDefaultValue(metadata.schema.columns[i].type);
                }
            }
        }

        for (size_t rowIdx = 0; rowIdx < rowGroup.numRows; rowIdx++)
        {
            ParquetRow row;
            foreach (colIdx; 0 .. metadata.schema.columns.length)
            {
                if (colIdx < columnData.length && rowIdx < columnData[colIdx].length)
                {
                    row ~= columnData[colIdx][rowIdx];
                }
                else
                {
                    row ~= getDefaultValue(metadata.schema.columns[colIdx].type);
                }
            }
            data ~= row;
        }
    }

    ParquetValue[] readColumnChunk(ColumnChunk columnChunk)
    {
        file.seek(columnChunk.fileOffset);

        ubyte[] buffer = new ubyte[cast(size_t) columnChunk.metaData.totalCompressedSize];
        file.rawRead(buffer);

        return deserializeColumnData(buffer, columnChunk.metaData.type, columnChunk
                .metaData.numValues);
    }

    ParquetValue[] deserializeColumnData(ubyte[] buffer, ParquetType type, long numValues)
    {
        ParquetValue[] values;
        size_t offset = 0;

        for (long i = 0; i < numValues && offset < buffer.length; i++)
        {
            ParquetValue value;
            size_t bytesRead = 0;

            final switch (type)
            {
            case ParquetType.BOOLEAN:
                if (offset < buffer.length)
                {
                    value = ParquetValue(buffer[offset] != 0);
                    bytesRead = 1;
                }
                break;

            case ParquetType.INT32:
                if (offset + 4 <= buffer.length)
                {
                    ubyte[4] intBytes = buffer[offset .. offset + 4];
                    value = ParquetValue(littleEndianToNative!int(intBytes));
                    bytesRead = 4;
                }
                break;

            case ParquetType.INT64:
                if (offset + 8 <= buffer.length)
                {
                    ubyte[8] longBytes = buffer[offset .. offset + 8];
                    value = ParquetValue(littleEndianToNative!long(longBytes));
                    bytesRead = 8;
                }
                break;

            case ParquetType.FLOAT:
                if (offset + 4 <= buffer.length)
                {
                    ubyte[4] floatBytes = buffer[offset .. offset + 4];
                    uint floatBits = littleEndianToNative!uint(floatBytes);
                    value = ParquetValue(*cast(float*)&floatBits);
                    bytesRead = 4;
                }
                break;

            case ParquetType.DOUBLE:
                if (offset + 8 <= buffer.length)
                {
                    ubyte[8] doubleBytes = buffer[offset .. offset + 8];
                    ulong doubleBits = littleEndianToNative!ulong(doubleBytes);
                    value = ParquetValue(*cast(double*)&doubleBits);
                    bytesRead = 8;
                }
                break;

            case ParquetType.BYTE_ARRAY:
                if (offset + 4 <= buffer.length)
                {
                    ubyte[4] lenBytes = buffer[offset .. offset + 4];
                    uint length = littleEndianToNative!uint(lenBytes);
                    offset += 4;
                    if (offset + length <= buffer.length)
                    {
                        value = ParquetValue(cast(string) buffer[offset .. offset + length]);
                        bytesRead = length;
                    }
                }
                break;

            case ParquetType.FIXED_LEN_BYTE_ARRAY:
                uint fixedLength = 16;
                if (offset + fixedLength <= buffer.length)
                {
                    value = ParquetValue(cast(string) buffer[offset .. offset + fixedLength]);
                    bytesRead = fixedLength;
                }
                break;
            }

            if (bytesRead == 0)
            {
                value = getDefaultValue(type);
            }

            values ~= value;
            offset += bytesRead;
        }

        return values;
    }

    void writeParquetFile()
    {
        file.rawWrite(cast(ubyte[]) PARQUET_MAGIC);

        long dataStart = file.tell();

        RowGroup rowGroup;
        rowGroup.numRows = data.length;
        rowGroup.columns = new ColumnChunk[metadata.schema.columns.length];

        long totalRowGroupSize = 0;

        foreach (colIdx, column; metadata.schema.columns)
        {
            long columnStart = file.tell();

            ParquetValue[] columnValues;
            foreach (row; data)
            {
                if (colIdx < row.length)
                {
                    columnValues ~= row[colIdx];
                }
                else
                {
                    columnValues ~= getDefaultValue(column.type);
                }
            }

            ubyte[] columnData = serializeColumnData(columnValues, column.type);
            file.rawWrite(columnData);

            ColumnChunk chunk;
            chunk.fileOffset = columnStart;
            chunk.metaData.type = column.type;
            chunk.metaData.encoding = ParquetEncoding.PLAIN;
            chunk.metaData.compression = ParquetCompression.UNCOMPRESSED;
            chunk.metaData.numValues = columnValues.length;
            chunk.metaData.totalUncompressedSize = columnData.length;
            chunk.metaData.totalCompressedSize = columnData.length;
            chunk.metaData.dataPageOffset = columnStart;

            rowGroup.columns[colIdx] = chunk;
            totalRowGroupSize += columnData.length;
        }

        rowGroup.totalByteSize = totalRowGroupSize;
        metadata.rowGroups = [rowGroup];

        ubyte[] footerData = createFooter();
        file.rawWrite(footerData);

        uint footerLength = cast(uint) footerData.length;
        ubyte[4] footerLengthBytes = nativeToLittleEndian(footerLength);
        file.rawWrite(footerLengthBytes);
        file.rawWrite(cast(ubyte[]) PARQUET_MAGIC);
    }

    ubyte[] serializeColumnData(ParquetValue[] values, ParquetType type)
    {
        ubyte[] result;

        foreach (value; values)
        {
            ubyte[] valueBytes = serializeValue(value, type);
            result ~= valueBytes;
        }

        return result;
    }

    ubyte[] serializeValue(ParquetValue value, ParquetType type)
    {
        final switch (type)
        {
        case ParquetType.BOOLEAN:
            if (auto boolPtr = value.peek!bool())
            {
                return [cast(ubyte)(*boolPtr ? 1 : 0)];
            }
            return [0];

        case ParquetType.INT32:
            if (auto intPtr = value.peek!int())
            {
                return nativeToLittleEndian(*intPtr).dup;
            }
            return nativeToLittleEndian(0).dup;

        case ParquetType.INT64:
            if (auto longPtr = value.peek!long())
            {
                return nativeToLittleEndian(*longPtr).dup;
            }
            return nativeToLittleEndian(0L).dup;

        case ParquetType.FLOAT:
            if (auto floatPtr = value.peek!float())
            {
                uint floatBits = *cast(uint*) floatPtr;
                return nativeToLittleEndian(floatBits).dup;
            }
            return nativeToLittleEndian(0u).dup;

        case ParquetType.DOUBLE:
            if (auto doublePtr = value.peek!double())
            {
                ulong doubleBits = *cast(ulong*) doublePtr;
                return nativeToLittleEndian(doubleBits).dup;
            }
            return nativeToLittleEndian(0UL).dup;

        case ParquetType.BYTE_ARRAY:
            string str;
            if (auto strPtr = value.peek!string())
            {
                str = *strPtr;
            }
            ubyte[] lengthBytes = nativeToLittleEndian(cast(uint) str.length).dup;
            return lengthBytes ~ cast(ubyte[]) str;

        case ParquetType.FIXED_LEN_BYTE_ARRAY:
            string str;
            if (auto strPtr = value.peek!string())
            {
                str = *strPtr;
            }
            ubyte[] result = cast(ubyte[]) str;
            result.length = 16;
            return result;
        }
    }

    ubyte[] createFooter()
    {
        ubyte[] footer;

        footer ~= nativeToLittleEndian(metadata.version_);
        footer ~= nativeToLittleEndian(cast(uint) metadata.schema.columns.length);

        foreach (column; metadata.schema.columns)
        {
            footer ~= nativeToLittleEndian(cast(uint) column.name.length);
            footer ~= cast(ubyte[]) column.name;
            footer ~= nativeToLittleEndian(cast(uint) column.type);
        }

        footer ~= nativeToLittleEndian(metadata.numRows);
        footer ~= nativeToLittleEndian(cast(uint) metadata.rowGroups.length);

        foreach (rowGroup; metadata.rowGroups)
        {
            footer ~= nativeToLittleEndian(rowGroup.numRows);
            footer ~= nativeToLittleEndian(rowGroup.totalByteSize);

            foreach (columnChunk; rowGroup.columns)
            {
                footer ~= nativeToLittleEndian(columnChunk.fileOffset);
                footer ~= nativeToLittleEndian(columnChunk.metaData.numValues);
                footer ~= nativeToLittleEndian(columnChunk.metaData.totalUncompressedSize);
            }
        }

        return footer;
    }

    ParquetValue getDefaultValue(ParquetType type)
    {
        final switch (type)
        {
        case ParquetType.BOOLEAN:
            return ParquetValue(false);
        case ParquetType.INT32:
            return ParquetValue(0);
        case ParquetType.INT64:
            return ParquetValue(0L);
        case ParquetType.FLOAT:
            return ParquetValue(0.0f);
        case ParquetType.DOUBLE:
            return ParquetValue(0.0);
        case ParquetType.BYTE_ARRAY:
        case ParquetType.FIXED_LEN_BYTE_ARRAY:
            return ParquetValue("");
        }
    }

    string convertParquetValueToString(ParquetValue value)
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

    ParquetValue convertStringToParquetValue(string strValue, ParquetType targetType)
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

    void validateRow(ParquetRow row)
    {
        foreach (i, value; row)
        {
            if (i >= metadata.schema.columns.length)
            {
                throw new ParquetWriteException("Too many columns in row");
            }

            auto expectedType = metadata.schema.columns[i].type;

            if (!validateValueType(value, expectedType))
            {
                throw new ParquetWriteException(
                    "Type mismatch for column '" ~ metadata.schema.columns[i].name ~
                        "', expected " ~ expectedType.to!string
                );
            }
        }
    }

    bool validateValueType(ParquetValue value, ParquetType expectedType)
    {
        final switch (expectedType)
        {
        case ParquetType.BOOLEAN:
            return value.peek!bool() !is null;
        case ParquetType.INT32:
            return value.peek!int() !is null;
        case ParquetType.INT64:
            return value.peek!long() !is null;
        case ParquetType.FLOAT:
            return value.peek!float() !is null;
        case ParquetType.DOUBLE:
            return value.peek!double() !is null;
        case ParquetType.BYTE_ARRAY:
        case ParquetType.FIXED_LEN_BYTE_ARRAY:
            return value.peek!string() !is null || value.peek!(ubyte[])() !is null;
        }
    }
}
