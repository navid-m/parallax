/**
 * Authors: Navid M
 * License: GPL3
 * Description: Contains parallax tests.
 */

module parallax.tests;

import parallax.columns;
import parallax.values;
import parallax.dataframes;
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

package unittest
{
    string[] dates = [
        "2023-01-15 09:30:00",
        "2023-02-20 14:45:30",
        "2023-03-25 16:20:15",
        "2023-04-10 11:10:45",
        "2023-05-05 13:55:20"
    ];

    double[] prices = [100.5, 105.2, 98.7, 110.3, 107.8];
    int[] volumes = [1000, 1500, 800, 2000, 1200];

    auto df = createDataFrame(
        ["date", "price", "volume"],
        dates, prices, volumes
    );

    writeln("Original DataFrame:");
    df.show();

    auto dfWithDates = df.toDatetime("date");
    writeln("\nAfter converting to datetime:");
    dfWithDates.show();

    auto dtCol = dfWithDates.dt("date");

    writeln("\nExtracting date components:");
    writeln("Years: ", dtCol.dt_year().getData());
    writeln("Months: ", dtCol.dt_month().getData());
    writeln("Days: ", dtCol.dt_day().getData());
    writeln("Day names: ");

    foreach (i; 0 .. dtCol.length)
    {
        auto dt = dtCol.getData()[i];
        writeln("  ", dt.dayName);
    }

    writeln("\nFormatted dates:");
    foreach (i; 0 .. dtCol.length)
    {
        auto dt = dtCol.getData()[i];
        writeln("  ", dt.strftime("%B %d, %Y at %H:%M"));
    }

    writeln;
}

package unittest
{
    string[] ids = ["A", "B", "C"];
    string[] val1 = ["10", "20", ""];
    string[] val2 = ["", "25", "35"];
    string[] val3 = ["15", "", "40"];

    auto dfWithMissing = createDataFrame(
        ["id", "metric1", "metric2", "metric3"],
        ids, val1, val2, val3
    );

    writeln("Data with missing values:");
    dfWithMissing.show();

    auto meltedWithMissing = dfWithMissing.meltAllExcept("id");
    writeln("\nMelted data (missing values preserved):");
    meltedWithMissing.show();
    writeln();
}

package unittest
{
    string[] hospitals = ["General", "Memorial", "Regional", "County"];
    int[] beds = [250, 180, 320, 150];
    int[] patients = [230, 165, 295, 140];
    double[] occupancy = [0.92, 0.92, 0.92, 0.93];
    double[] satisfaction = [4.2, 4.5, 3.8, 4.1];

    auto hospitalDf = createDataFrame(
        [
        "hospital", "beds", "patients", "occupancy_rate", "patient_satisfaction"
    ],
        hospitals, beds, patients, occupancy, satisfaction
    );

    writeln("Original hospital data:");
    hospitalDf.show();

    auto capacityMelted = hospitalDf.melt(["hospital"], ["beds", "patients"],
        "capacity_type", "count");
    writeln("\nCapacity metrics (melted):");
    capacityMelted.show();

    auto qualityMelted = hospitalDf.melt(["hospital"], [
        "occupancy_rate", "patient_satisfaction"
    ],
    "quality_metric", "score");
    writeln("\nQuality metrics (melted):");
    qualityMelted.show();
}
