module dataframes;

import std.stdio;
import std.string;
import std.array;
import std.conv;
import std.algorithm;
import std.range;
import std.parallelism;
import std.file;
import std.csv;
import std.variant;
import std.typecons;
import std.traits;
import std.exception;

alias DataValue = Variant;

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

class TypedColumn(T) : IColumn
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
		return to!string(col[idx]);
	}

	IColumn slice(size_t start, size_t end)
	{
		return new TypedColumn!T(col.name, col.data[start .. end]);
	}

	IColumn copy()
	{
		return new TypedColumn!T(col.name, col.data.dup);
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

class DataFrame
{
	private IColumn[] columns_;
	private string[] columnNames_;
	private size_t[string] nameToIndex_;

	this()
	{
	}

	this(IColumn[] cols)
	{
		foreach (col; cols)
		{
			addColumn(col);
		}
	}

	static DataFrame create(T...)(string[] names, T arrays)
	{
		auto df = new DataFrame();
		foreach (i, arr; arrays)
		{
			static if (isArray!(typeof(arr)))
			{
				alias ElementType = typeof(arr[0]);
				df.addColumn(new TypedColumn!ElementType(names[i], arr));
			}
		}
		return df;
	}

	private void addColumn(IColumn col)
	{
		columns_ ~= col;
		columnNames_ ~= col.name;
		nameToIndex_[col.name] = cast(int) columns_.length - 1;
	}

	IColumn opIndex(string colName)
	{
		if (colName !in nameToIndex_)
		{
			throw new Exception("Column '" ~ colName ~ "' not found");
		}
		return columns_[nameToIndex_[colName]];
	}

	DataValue opIndex(size_t row, string col)
	{
		return this[col].getValue(row);
	}

	DataValue opIndex(size_t row, size_t col)
	{
		return columns_[col].getValue(row);
	}

	void opIndexAssign(DataValue value, size_t row, string col)
	{
		this[col].setValue(row, value);
	}

	void opIndexAssign(DataValue value, size_t row, size_t col)
	{
		columns_[col].setValue(row, value);
	}

	@property size_t rows() const
	{
		return columns_.length > 0 ? columns_[0].length : 0;
	}

	@property size_t cols() const
	{
		return columns_.length;
	}

	@property string[] columns() const
	{
		return columnNames_.dup;
	}

	@property Tuple!(size_t, size_t) shape() const
	{
		return tuple(rows, cols);
	}

	DataFrame opSlice(size_t rowStart, size_t rowEnd)
	{
		auto newCols = columns_.map!(col => col.slice(rowStart, rowEnd)).array;
		return new DataFrame(newCols);
	}

	DataFrame head(size_t n = 5)
	{
		size_t end = std.algorithm.min(n, rows);
		return this[0 .. end];
	}

	DataFrame tail(size_t n = 5)
	{
		size_t start = rows > n ? rows - n : 0;
		return this[start .. rows];
	}

	DataFrame select(string[] colNames...)
	{
		IColumn[] selectedCols;
		foreach (name; colNames)
		{
			selectedCols ~= this[name].copy();
		}
		return new DataFrame(selectedCols);
	}

	DataFrame where(bool[] mask)
	{
		enforce(mask.length == rows, "Mask length must match number of rows");

		IColumn[] filteredCols;
		foreach (col; columns_)
		{
			auto newCol = col.copy();
			// TODO: Implement proper filtering.
			filteredCols ~= newCol;
		}
		return new DataFrame(filteredCols);
	}

	DataFrame groupBy(string colName)
	{
		// TODO: Actually implement groupBy.
		return new DataFrame(columns_.map!(col => col.copy()).array);
	}

	DataFrame describe()
	{
		if (rows == 0)
			return new DataFrame();

		string[] statNames = [
			"count", "mean", "std", "min", "25%", "50%", "75%", "max"
		];
		IColumn[] resultCols;
		auto indexCol = new TypedColumn!string("", statNames.dup);
		resultCols ~= cast(IColumn) indexCol;

		foreach (col; columns_)
		{
			auto stringCol = cast(TypedColumn!string) col;
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

				if (!isNumeric)
					continue;

				auto stats = calculateStats(numericData);
				auto statCol = new TypedColumn!string(col.name, stats);
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
					auto statCol = new TypedColumn!string(col.name, stats);
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

		auto valueCol = new TypedColumn!string(colName, sortedValues);
		auto countCol = new TypedColumn!int("count", sortedCounts);

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
				auto stringCol = cast(TypedColumn!string) col;
				if (stringCol)
				{
					auto newCol = new TypedColumn!string(col.name);
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
		auto indexCol = new TypedColumn!string(index, indexValues.dup);
		resultCols ~= cast(IColumn) indexCol;

		foreach (colVal; columnValues)
		{
			auto pivotCol = new TypedColumn!string(colVal);

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

	DataFrame apply(T)(T delegate(string[]) func, int axis = 0)
	{
		if (axis == 0)
		{
			auto resultCol = new TypedColumn!string("result");

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
				auto resultCol = new TypedColumn!string(col.name, [result]);
				resultCols ~= cast(IColumn) resultCol;
			}

			return new DataFrame(resultCols);
		}
	}

	private import std.format : format;
	private import std.algorithm : minElement, maxElement, filter, canFind;

	void showPivot(size_t maxRows = 20, size_t maxCols = 10)
	{
		writeln("DataFrame(", rows, " rows, ", cols, " columns)");

		size_t displayCols = std.algorithm.min(maxCols, cols);
		size_t displayRows = std.algorithm.min(maxRows, rows);

		size_t[] colWidths = new size_t[displayCols];
		foreach (i; 0 .. displayCols)
		{
			colWidths[i] = std.algorithm.max(columnNames_[i].length, 8);
			foreach (j; 0 .. displayRows)
			{
				colWidths[i] = std.algorithm.max(colWidths[i], columns_[i].toString(j).length);
			}
		}

		write("   ");
		foreach (i; 0 .. displayCols)
		{
			writef("%-*s ", colWidths[i], columnNames_[i]);
		}
		writeln();
		write("   ");
		foreach (i; 0 .. displayCols)
		{
			version (Windows)
			{
				import core.sys.windows.windows : SetConsoleOutputCP;

				SetConsoleOutputCP(65_001);
			}
			foreach (j; 0 .. colWidths[i])
				write("─");
			write(" ");
		}
		writeln();

		foreach (i; 0 .. displayRows)
		{
			writef("%3d ", i);
			foreach (j; 0 .. displayCols)
			{
				writef("%-*s ", colWidths[j], columns_[j].toString(i));
			}
			writeln();
		}

		if (rows > maxRows)
			writeln("... (", rows - maxRows, " more rows)");
		if (cols > maxCols)
			writeln("... (", cols - maxCols, " more columns)");
	}

	void show(size_t maxRows = 10)
	{
		writeln("DataFrame(", rows, " rows, ", cols, " columns)");

		write("   ");
		foreach (name; columnNames_)
		{
			writef("%12s ", name);
		}
		writeln();

		size_t displayRows = std.algorithm.min(maxRows, rows);
		foreach (i; 0 .. displayRows)
		{
			writef("%3d ", i);
			foreach (col; columns_)
			{
				writef("%12s ", col.toString(i));
			}
			writeln();
		}

		if (rows > maxRows)
		{
			writeln("... (", rows - maxRows, " more rows)");
		}
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
			foreach (i; 0 .. firstLine.length)
			{
				headers ~= "col" ~ to!string(i);
			}
		}

		auto dataLines = lines[dataStart .. $];
		auto numThreads = totalCPUs;
		auto chunkSize = std.algorithm.max(1, dataLines.length / numThreads);
		auto columns = new TypedColumn!string[](headers.length);

		foreach (i, header; headers)
			columns[i] = new TypedColumn!string(header);

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
				foreach (i, field; fields)
				{
					if (i < headers.length)
					{
						results[chunkIdx][i] ~= field.strip;
					}
				}
			}
		}

		foreach (colIdx; 0 .. headers.length)
		{
			foreach (result; results)
			{
				if (colIdx < result.length)
				{
					foreach (value; result[colIdx])
					{
						columns[colIdx].append(value);
					}
				}
			}
		}

		IColumn[] finalCols;
		foreach (col; columns)
		{
			finalCols ~= cast(IColumn) col;
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

	DataFrame sortValues(string colName, bool ascending = true)
	{
		// TODO: Sort properly.
		auto colIdx = nameToIndex_[colName];
		auto indices = iota(0, rows).array;

		indices.sort!((a, b) => columns_[colIdx].toString(a) < columns_[colIdx].toString(b));

		if (!ascending)
		{
			indices.reverse();
		}

		auto newCols = new IColumn[](cols);
		foreach (i, col; columns_)
		{
			newCols[i] = col.copy();
		}

		return new DataFrame(newCols);
	}

	DataFrame sum()
	{
		if (rows == 0)
			return new DataFrame();

		IColumn[] resultCols;
		foreach (col; columns_)
		{
			try
			{
				auto stringCol = cast(TypedColumn!string) col;
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
				resultCols ~= new TypedColumn!double(col.name, [total]);
			}
			catch (Exception e)
			{
				continue;
			}
		}
		return new DataFrame(resultCols);
	}

	DataFrame mean()
	{
		if (rows == 0)
			return new DataFrame();

		IColumn[] resultCols;
		foreach (col; columns_)
		{
			try
			{
				auto stringCol = cast(TypedColumn!string) col;
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
					resultCols ~= new TypedColumn!double(col.name, [
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

	DataFrame max()
	{
		if (rows == 0)
			return new DataFrame();

		IColumn[] resultCols;
		foreach (col; columns_)
		{
			try
			{
				auto stringCol = cast(TypedColumn!string) col;
				if (stringCol)
				{
					string maxVal = stringCol.getData()[0];
					foreach (val; stringCol.getData()[1 .. $])
					{
						if (val > maxVal)
							maxVal = val;
					}
					resultCols ~= new TypedColumn!string(col.name, [maxVal]);
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
					resultCols ~= new TypedColumn!double(col.name, [maxVal]);
				}
			}
			catch (Exception e)
			{
				continue;
			}
		}
		return new DataFrame(resultCols);
	}

	DataFrame min()
	{
		if (rows == 0)
			return new DataFrame();

		IColumn[] resultCols;
		foreach (col; columns_)
		{
			try
			{
				auto stringCol = cast(TypedColumn!string) col;
				if (stringCol)
				{
					string minVal = stringCol.getData()[0];
					foreach (val; stringCol.getData()[1 .. $])
					{
						if (val < minVal)
							minVal = val;
					}
					resultCols ~= new TypedColumn!string(col.name, [minVal]);
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
					resultCols ~= new TypedColumn!double(col.name, [minVal]);
				}
			}
			catch (Exception e)
			{
				continue;
			}
		}
		return new DataFrame(resultCols);
	}

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
			auto newCol = new TypedColumn!string(col.name);
			resultCols ~= cast(IColumn) newCol;
		}

		foreach (col; other.columns_)
		{
			if (col.name != on)
			{
				auto newCol = new TypedColumn!string(col.name);
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
						auto typedCol = cast(TypedColumn!string) resultCols[i];
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
							auto typedCol = cast(TypedColumn!string) resultCols[resultColIdx];
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

	DataFrame copy()
	{
		auto newCols = columns_.map!(col => col.copy()).array;
		return new DataFrame(newCols);
	}

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
			auto stringCol = cast(TypedColumn!string) col;
			if (stringCol)
			{
				auto newCol = new TypedColumn!string(col.name);
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
				// For other types, create string representation
				auto newCol = new TypedColumn!string(col.name);
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

	DataFrame rename(string[string] mapping)
	{
		auto newCols = new IColumn[](cols);
		foreach (i, col; columns_)
		{
			auto newCol = col.copy();
			if (col.name in mapping)
			{
				// TODO: need to update column name in copy
			}
			newCols[i] = newCol;
		}
		return new DataFrame(newCols);
	}
}

DataFrame createDataFrame(T...)(string[] names, T data) => DataFrame.create(names, data);

unittest
{
	auto ages = [25, 30, 35, 40];
	auto names = ["Alice", "Bob", "Charlie", "David"];
	auto salaries = [50_000.0, 60_000.0, 70_000.0, 80_000.0];
	auto df = createDataFrame(["name", "age", "salary"], names, ages, salaries);

	assert(df.rows == 4);
	assert(df.cols == 3);

	auto head = df.head(2);
	assert(head.rows == 2);

	auto selected = df.select("name", "age");
	assert(selected.cols == 2);

}

class FastCsvReader
{
	static DataFrame readLargeCsv(string filename, bool hasHeader = true)
	{
		auto file = File(filename, "r");
		scope (exit)
			file.close();
		enum bufferSize = 64 * 1024;
		auto buffer = new char[bufferSize];
		return new DataFrame();
	}
}

void main()
{
}

unittest
{

	auto names = ["Alice", "Bob", "Charlie", "David", "Eve", "Alice", "Bob"];
	auto ages = ["25", "30", "35", "40", "28", "25", "32"];
	auto salaries = [
		"50000", "60000", "70000", "80000", "55000", "50000", "65000"
	];
	auto departments = ["IT", "HR", "IT", "Finance", "IT", "HR", "HR"];
	auto nameCol = new TypedColumn!string("name", names.dup);
	auto ageCol = new TypedColumn!string("age", ages.dup);
	auto salaryCol = new TypedColumn!string("salary", salaries.dup);
	auto deptCol = new TypedColumn!string("department", departments.dup);
	auto df = new DataFrame([
		cast(IColumn) nameCol, cast(IColumn) ageCol,
		cast(IColumn) salaryCol, cast(IColumn) deptCol
	]);

	writeln("Original DataFrame:");
	df.show();

	writeln("\n1. DESCRIBE() - Statistical Summary:");
	auto desc = df.describe();
	desc.showPivot();

	writeln("\n2. VALUE_COUNTS() - Count unique names:");
	auto counts = df.valueCounts("name");
	counts.show();

	writeln("\n3. FILLNA() - Fill missing values (demo with copy):");
	auto filled = df.fillna(DataValue("Unknown"));
	writeln("(No missing values in this example, but function is ready)");

	writeln("\n4. PIVOT_TABLE() - Average salary by department and name:");
	auto pivot = df.pivotTable("salary", "department", "name", "mean");
	pivot.showPivot();

	writeln("\n5. APPLY() - Custom function on rows (count non-empty fields):");
	auto applied = df.apply((string[] row) {
		return row.filter!(x => x.length > 0).array.length;
	});
	applied.show();
}
