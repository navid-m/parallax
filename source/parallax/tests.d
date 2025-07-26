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

package unittest
{
    import std.file;
    import std.format;

    auto names = ["Alice Johnson", "Bob Smith", "Charlie Brown", "Diana Prince"];
    auto ages = [25, 30, 35, 28];
    auto salaries = [50_000.0, 65_000.0, 75_000.0, 58_000.0];
    auto departments = ["Engineering", "Marketing", "Finance", "HR"];
    auto originalDf = createDataFrame(
        ["name", "age", "salary", "department"],
        names, ages, salaries, departments
    );

    writeln("Original DataFrame:");
    originalDf.show();
    string testFile = "test_basic.csv";
    scope (exit)
        if (exists(testFile))
            remove(testFile);

    auto sw = StopWatch(AutoStart.yes);
    originalDf.toCsv(testFile);
    writeln("CSV write time: ", sw.peek.total!"msecs", " ms");

    assert(exists(testFile), "CSV file was not created");
    writeln("CSV file size: ", getSize(testFile), " bytes");

    sw.reset();
    sw.start();
    auto loadedDf = DataFrame.readCsv(testFile);
    writeln("CSV read time: ", sw.peek.total!"msecs", " ms");

    writeln("\nLoaded DataFrame:");
    loadedDf.show();

    auto originalNames = originalDf.columns();
    auto loadedNames = loadedDf.columns();
    loadedDf.show();
}

package unittest
{
    import std.file;
    import std.format;
    import std.datetime.stopwatch;
    import std.stdio : writeln;

    auto names = ["Alice Johnson", "Bob Smith", "Charlie Brown", "Diana Prince"];
    auto ages = [25, 30, 35, 28];
    auto salaries = [50_000.0, 65_000.0, 75_000.0, 58_000.0];
    auto departments = ["Engineering", "Marketing", "Finance", "HR"];

    auto originalDf = createDataFrame(
        ["name", "age", "salary", "department"],
        names, ages, salaries, departments
    );

    writeln("Original DataFrame:");
    originalDf.show();

    string testFile = "test_basic.parquet";
    scope (exit)
        if (exists(testFile))
            remove(testFile);

    writeln("\nParquet Info for Original DataFrame:");
    originalDf.parquetInfo();

    auto sw = StopWatch(AutoStart.yes);
    originalDf.toParquet(testFile);
    writeln("Parquet write time: ", sw.peek.total!"msecs", " ms");

    assert(exists(testFile), "Parquet file was not created");
    writeln("Parquet file size: ", getSize(testFile), " bytes");

    sw.reset();
    sw.start();
    auto loadedDf = DataFrame.readParquet(testFile);
    writeln("Parquet read time: ", sw.peek.total!"msecs", " ms");

    writeln("\nLoaded DataFrame:");
    loadedDf.show();

    auto originalNames = originalDf.columns();
    auto loadedNames = loadedDf.columns();

    writeln("\nData Integrity Checks:");
    writeln("Original columns: ", originalNames);
    writeln("Loaded columns: ", loadedNames);

    assert(originalNames.length == loadedNames.length,
        format("Column count mismatch: original=%d, loaded=%d",
            originalNames.length, loadedNames.length));

    foreach (i, origName; originalNames)
    {
        assert(origName == loadedNames[i],
            format("Column name mismatch at index %d: '%s' vs '%s'",
                i, origName, loadedNames[i]));
    }

    assert(originalDf.shape == loadedDf.shape,
        format("DataFrame shape mismatch: original=%s, loaded=%s",
            originalDf.shape, loadedDf.shape));

    writeln("Rows match: ", originalDf.rows == loadedDf.rows);
    writeln("Columns match: ", originalDf.cols == loadedDf.cols);
    writeln("\nParquet Info for Loaded DataFrame:");
    loadedDf.parquetInfo();
    writeln("\n=== Testing Edge Cases ===");
    auto emptyDf = new DataFrame();
    string emptyTestFile = "test_empty.parquet";
    scope (exit)
        if (exists(emptyTestFile))
            remove(emptyTestFile);

    try
    {
        emptyDf.toParquet(emptyTestFile);
        assert(false, "Should have thrown exception for empty DataFrame");
    }
    catch (Exception e)
    {
        writeln("✓ Empty DataFrame correctly rejected: ", e.msg);
    }

    auto singleNames = ["Single User"];
    auto singleAges = [42];
    auto singleSalaries = [80_000.0];
    auto singleDepartments = ["IT"];

    auto singleRowDf = createDataFrame(
        ["name", "age", "salary", "department"],
        singleNames, singleAges, singleSalaries, singleDepartments
    );

    string singleRowFile = "test_single_row.parquet";
    scope (exit)
        if (exists(singleRowFile))
            remove(singleRowFile);

    writeln("\nTesting single row DataFrame:");
    singleRowDf.show();

    sw.reset();
    sw.start();
    singleRowDf.toParquet(singleRowFile);
    writeln("Single row write time: ", sw.peek.total!"msecs", " ms");

    sw.reset();
    sw.start();
    auto loadedSingleRowDf = DataFrame.readParquet(singleRowFile);
    writeln("Single row read time: ", sw.peek.total!"msecs", " ms");

    writeln("Loaded single row DataFrame:");
    loadedSingleRowDf.show();

    assert(singleRowDf.shape == loadedSingleRowDf.shape, "Single row DataFrame shape mismatch");
    writeln("✓ Single row DataFrame test passed");

    auto mixedNames = ["Test User", "Another User", "", "Null User"];
    auto mixedAges = [0, -1, 999, 25];
    auto mixedSalaries = [0.0, -1000.0, 1_000_000.0, 45_000.5];
    auto mixedDepartments = [
        "", "Special Chars: éñ中",
        "Very Long Department Name That Exceeds Normal Limits", "Normal"
    ];

    auto mixedDf = createDataFrame(
        ["name", "age", "salary", "department"],
        mixedNames, mixedAges, mixedSalaries, mixedDepartments
    );

    string mixedTestFile = "test_mixed.parquet";
    scope (exit)
        if (exists(mixedTestFile))
            remove(mixedTestFile);

    writeln("\nTesting DataFrame with mixed/special values:");
    mixedDf.show();
    mixedDf.parquetInfo();

    sw.reset();
    sw.start();
    mixedDf.toParquet(mixedTestFile);
    writeln("Mixed data write time: ", sw.peek.total!"msecs", " ms");

    sw.reset();
    sw.start();
    auto loadedMixedDf = DataFrame.readParquet(mixedTestFile);
    writeln("Mixed data read time: ", sw.peek.total!"msecs", " ms");

    writeln("Loaded mixed DataFrame:");
    loadedMixedDf.show();

    assert(mixedDf.shape == loadedMixedDf.shape, "Mixed DataFrame shape mismatch");
    writeln("✓ Mixed data DataFrame test passed");

    writeln("\n=== Performance Comparison: Parquet vs CSV ===");

    string csvCompareFile = "test_compare.csv";
    string parquetCompareFile = "test_compare.parquet";
    scope (exit)
    {
        if (exists(csvCompareFile))
            remove(csvCompareFile);
        if (exists(parquetCompareFile))
            remove(parquetCompareFile);
    }

    sw.reset();
    sw.start();
    originalDf.toCsv(csvCompareFile);
    auto csvWriteTime = sw.peek.total!"msecs";

    sw.reset();
    sw.start();
    auto csvLoadedDf = DataFrame.readCsv(csvCompareFile);
    auto csvReadTime = sw.peek.total!"msecs";

    sw.reset();
    sw.start();
    originalDf.toParquet(parquetCompareFile);
    auto parquetWriteTime = sw.peek.total!"msecs";

    sw.reset();
    sw.start();
    auto parquetLoadedDf = DataFrame.readParquet(parquetCompareFile);
    auto parquetReadTime = sw.peek.total!"msecs";
    auto csvSize = getSize(csvCompareFile);
    auto parquetSize = getSize(parquetCompareFile);

    writeln("Performance Results:");
    writeln(format("CSV    - Write: %d ms, Read: %d ms, Size: %d bytes",
            csvWriteTime, csvReadTime, csvSize));
    writeln(format("Parquet - Write: %d ms, Read: %d ms, Size: %d bytes",
            parquetWriteTime, parquetReadTime, parquetSize));
    writeln(format("Size ratio (Parquet/CSV): %.2f",
            cast(double) parquetSize / csvSize));

    writeln("\n=== Testing Error Handling ===");

    try
    {
        auto nonExistentDf = DataFrame.readParquet("non_existent_file.parquet");
        assert(false, "Should have thrown exception for non-existent file");
    }
    catch (Exception e)
    {
        writeln("✓ Non-existent file correctly handled: ", e.msg);
    }

    writeln("\n✓ All Parquet tests completed successfully!");
}
