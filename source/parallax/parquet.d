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
        uint footerLength = littleEndianToNative!uint(footerLengthBytes);

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
        // TODO: Parse thrift properly.
        size_t offset = 0;

        if (footerData.length < 20)
        {
            throw new ParquetReadException("Footer too small");
        }

        metadata.version_ = PARQUET_VERSION;
        metadata.createdBy = "Unknown";
        metadata.numRows = 0;
        metadata.schema = TableSchema();

        inferSchemaFromFile();
    }

    void inferSchemaFromFile()
    {
        file.seek(4);
        data = [];
        try
        {
            readSimplifiedData();
        }
        catch (Exception e)
        {
            metadata.schema.addColumn("data", ParquetType.BYTE_ARRAY);
        }
    }

    void readSimplifiedData()
    {
        file.seek(4);
        long footerStart = file.size - 8 - 4;
        string content;
        ubyte[] buffer = new ubyte[cast(size_t)(footerStart - 4)];
        try
        {
            file.rawRead(buffer);
            try
            {
                content = cast(string) buffer;
                parseAsTabSeparated(content);
                return;
            }
            catch (Exception)
            {
            }
            parseAsBinaryColumns(buffer);

        }
        catch (Exception e)
        {
            throw new ParquetReadException("Could not parse data: " ~ e.msg);
        }
    }

    void parseAsTabSeparated(string content)
    {
        auto lines = content.splitLines();
        if (lines.length < 2)
            return;

        auto headers = lines[0].split('\t');
        metadata.schema = TableSchema();

        string[][] allRows;
        foreach (i; 1 .. lines.length)
        {
            auto fields = lines[i].split('\t');
            if (fields.length == headers.length)
            {
                allRows ~= fields;
            }
        }

        foreach (colIdx, header; headers)
        {
            auto inferredType = inferTypeFromColumn(allRows, colIdx);
            metadata.schema.addColumn(header, inferredType);
        }

        data = [];
        foreach (row; allRows)
        {
            ParquetRow parquetRow;
            foreach (colIdx, value; row)
            {
                if (colIdx < metadata.schema.columns.length)
                {
                    auto parquetValue = convertStringToParquetValue(value, metadata
                            .schema.columns[colIdx].type);
                    parquetRow ~= parquetValue;
                }
            }
            data ~= parquetRow;
        }

        metadata.numRows = data.length;
    }

    void parseAsBinaryColumns(ubyte[] buffer)
    {
        metadata.schema = TableSchema();
        metadata.schema.addColumn("binary_data", ParquetType.BYTE_ARRAY);
        size_t chunkSize = 1024; // Arbitrary chunk size
        data = [];

        for (size_t offset = 0; offset < buffer.length; offset += chunkSize)
        {
            size_t end = min(offset + chunkSize, buffer.length);
            auto chunk = buffer[offset .. end];

            ParquetRow row;
            row ~= ParquetValue(cast(string) chunk);
            data ~= row;
        }

        metadata.numRows = data.length;
    }

    void readRowGroups()
    {
        // TODO:
        // 1. Read each row group's column chunks
        // 2. Decompress if needed
        // 3. Decode based on encoding type
        // 4. Reconstruct rows from columnar data
    }

    void writeParquetFile()
    {
        file.rawWrite(cast(ubyte[]) PARQUET_MAGIC);
        writeDataSection();
        ubyte[] footerData = createFooter();
        file.rawWrite(footerData);
        uint footerLength = cast(uint) footerData.length;
        ubyte[4] footerLengthBytes = nativeToLittleEndian(footerLength);
        file.rawWrite(footerLengthBytes);
        file.rawWrite(cast(ubyte[]) PARQUET_MAGIC);
    }

    void writeDataSection()
    {
        string[] headers;
        foreach (col; metadata.schema.columns)
        {
            headers ~= col.name;
        }
        string headerLine = headers.join("\t") ~ "\n";
        file.rawWrite(cast(ubyte[]) headerLine);
        foreach (row; data)
        {
            string[] values;
            foreach (value; row)
            {
                values ~= convertParquetValueToString(value);
            }
            string dataLine = values.join("\t") ~ "\n";
            file.rawWrite(cast(ubyte[]) dataLine);
        }
    }

    ubyte[] createFooter()
    {
        string footerJson = `{
            "version": `
            ~ metadata.version_.to!string ~ `,
            "num_rows": `
            ~ metadata.numRows.to!string ~ `,
            "created_by": "`
            ~ metadata.createdBy ~ `",
            "schema": [`;

        foreach (i, col; metadata.schema.columns)
        {
            if (i > 0)
                footerJson ~= ",";
            footerJson ~= `{"name": "` ~ col.name ~ `", "type": "` ~ col.type.to!string ~ `"}`;
        }

        footerJson ~= `]}`;

        return cast(ubyte[]) footerJson;
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

    ParquetType inferTypeFromColumn(string[][] allRows, size_t colIdx)
    {
        import std.conv : to, ConvException;
        import std.string : strip, toLower;
        import std.algorithm : min;

        if (allRows.length == 0 || colIdx >= allRows[0].length)
        {
            return ParquetType.BYTE_ARRAY;
        }

        size_t sampleSize = min(100, allRows.length);
        int intCount = 0;
        int floatCount = 0;
        int boolCount = 0;

        foreach (i; 0 .. sampleSize)
        {
            if (colIdx >= allRows[i].length)
                continue;

            string value = allRows[i][colIdx].strip().toLower();
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
