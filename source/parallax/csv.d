module parallax.csv;

import parallax.dataframes;
import std.stdio;

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
