module warthog.benchmarks;

import warthog.columns;
import warthog.dataframes;
import warthog.values;
import std.datetime.stopwatch;
import std.stdio;
import std.range;
import std.array;
import std.algorithm;

void benches_one()
{
	auto names = ["Alice", "Bob", "Charlie", "David", "Eve", "Alice", "Bob"];
	auto ages = ["25", "30", "35", "40", "28", "25", "32"];
	auto salaries = [
		"50000", "60000", "70000", "80000", "55000", "50000", "65000"
	];
	auto departments = ["IT", "HR", "IT", "Finance", "IT", "HR", "HR"];

	auto nameCol = new TCol!string("name", names.dup);
	auto ageCol = new TCol!string("age", ages.dup);
	auto salaryCol = new TCol!string("salary", salaries.dup);
	auto deptCol = new TCol!string("department", departments.dup);
	auto df = new DataFrame([
		cast(IColumn) nameCol, cast(IColumn) ageCol,
		cast(IColumn) salaryCol, cast(IColumn) deptCol
	]);

	auto sw = StopWatch(AutoStart.yes);

	sw.reset();
	sw.start();
	df.describe().show();
	writeln("Describe: ", sw.peek.total!"msecs", " ms");

	sw.reset();
	sw.start();
	df.valueCounts("name");
	writeln("Value counts: ", sw.peek.total!"msecs", " ms");

	sw.reset();
	sw.start();
	df.fillna(DataValue("Unknown"));
	writeln("FillNA: ", sw.peek.total!"msecs", " ms");

	sw.reset();
	sw.start();
	df.pivotTable("salary", "department", "name", "mean");
	writeln("Pivot Table: ", sw.peek.total!"msecs", " ms");

	sw.reset();
	sw.start();
	df.apply((string[] row) { return row.filter!(x => x.length > 0).array.length; });
	writeln("Apply: ", sw.peek.total!"msecs", " ms");
}

void benches_two()
{
	auto sw = StopWatch(AutoStart.yes);
	auto count = 10_000_000;
	auto ids = iota(0, count).array;
	auto values = ids.map!(i => cast(double)(i) * 1.5).array;
	auto idCol = new TCol!int("id", ids);
	auto valueCol = new TCol!double("value", values);
	auto df = new DataFrame([
		cast(IColumn) idCol,
		cast(IColumn) valueCol
	]);
	writeln("Created in ", sw.peek.total!"seconds", " seconds");
}

void main()
{
	benches_two();
}
