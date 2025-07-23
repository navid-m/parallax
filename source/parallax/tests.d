module parallax.tests;

import parallax.columns;
import parallax.values;
import parallax.dataframes;
import parallax.csv;
import std.array;
import std.algorithm;
import std.stdio;
import std.datetime.stopwatch;
import std.range;

package unittest
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

package unittest
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

package unittest
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

    writeln("\n--- groupBy(\"department\") ---");
    auto grouped = df.groupBy("department");
    writeln(grouped);

    auto sw2 = StopWatch(AutoStart.yes);
    auto count = 10_000_000;
    auto ids = iota(0, count).array;
    auto values = ids.map!(i => cast(double)(i) * 1.5).array;
    auto idCol = new TCol!int("id", ids);
    auto valueCol = new TCol!double("value", values);
    auto df2 = new DataFrame([
        cast(IColumn) idCol,
        cast(IColumn) valueCol
    ]);
    writeln("Created in ", sw2.peek.total!"seconds", " seconds");
}
