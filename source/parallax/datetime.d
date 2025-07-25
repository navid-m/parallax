/**
 * Authors: Navid M
 * License: GPL3
 * Description: Contains operations for datetime handling.
 */

module parallax.datetime;

import std.datetime;
import std.datetime.systime;
import std.datetime.timezone;
import std.conv;
import std.string;
import std.regex;
import std.array;
import std.algorithm;
import std.exception;
import std.variant;
import std.typecons;
import std.format;
import std.range;
import parallax.values;
import parallax.columns;

/**
 * A date and time with millisecond precision.
 */
struct ParallaxDateTime
{
    SysTime timestamp;

    /**
     * Construct a ParallaxDateTime from a SysTime object.
     *
     * Params:
     *   dt = The `SysTime` object.
     */
    this(SysTime dt)
    {
        timestamp = dt;
    }

    /**
     * Construct a ParallaxDateTime from a DateTime object and an optional TimeZone.
     *
     * Params:
     *   dt = The `DateTime` object.
     *   tz = The `TimeZone` (defaults to UTC).
     */
    this(DateTime dt, immutable TimeZone tz = UTC())
    {
        timestamp = SysTime(dt, tz);
    }

    /**
     * Construct a date time object from individual date and time components.
     *
     * Params:
     *   year = The year.
     *   month = The month (1-12).
     *   day = The day of the month (1-31).
     *   hour = The hour (0-23, defaults to 0).
     *   minute = The minute (0-59, defaults to 0).
     *   second = The second (0-59, defaults to 0).
     *   msecs = The milliseconds (0-999, defaults to 0).
     */
    this(int year, int month, int day, int hour = 0, int minute = 0, int second = 0, int msecs = 0)
    {
        auto dt = DateTime(year, month, day, hour, minute, second);
        timestamp = SysTime(dt, msecs.msecs, UTC());
    }

    /**
     * Get the year component of the datetime.
     */
    @property int year() const => timestamp.year;

    /**
     * Get the month component of the datetime (1-12).
     */
    @property int month() const => cast(int) timestamp.month;

    /**
     * Get the day component of the datetime (1-31).
     */
    @property int day() const => timestamp.day;

    /**
     * Get the hour component of the datetime (0-23).
     */
    @property int hour() const => timestamp.hour;

    /**
     * Get the minute component of the datetime (0-59).
     */
    @property int minute() const => timestamp.minute;

    /**
     * Get the second component of the datetime (0-59).
     */
    @property int second() const => timestamp.second;

    /**
     * Get the millisecond component of the datetime (0-999).
     */
    @property int millisecond() const => cast(int) timestamp.fracSecs.total!"msecs";

    /**
     * Get the day of the week (0 for Sunday, 6 for Saturday).
     */
    @property int dayOfWeek() const => cast(int) timestamp.dayOfWeek;

    /**
     * Get the day of the year (1-366).
     */
    @property int dayOfYear() const => timestamp.dayOfYear;

    /**
     * Get the full name of the day of the week.
     */
    @property string dayName() const
    {
        final switch (timestamp.dayOfWeek)
        {
        case DayOfWeek.sun:
            return "Sunday";
        case DayOfWeek.mon:
            return "Monday";
        case DayOfWeek.tue:
            return "Tuesday";
        case DayOfWeek.wed:
            return "Wednesday";
        case DayOfWeek.thu:
            return "Thursday";
        case DayOfWeek.fri:
            return "Friday";
        case DayOfWeek.sat:
            return "Saturday";
        }
    }

    /**
     * Get the full name of the month.
     */
    @property string monthName() const
    {
        final switch (timestamp.month)
        {
        case Month.jan:
            return "January";
        case Month.feb:
            return "February";
        case Month.mar:
            return "March";
        case Month.apr:
            return "April";
        case Month.may:
            return "May";
        case Month.jun:
            return "June";
        case Month.jul:
            return "July";
        case Month.aug:
            return "August";
        case Month.sep:
            return "September";
        case Month.oct:
            return "October";
        case Month.nov:
            return "November";
        case Month.dec:
            return "December";
        }
    }

    /**
     * Adds or subtracts a Duration from the ParallaxDateTime.
     *
     * Params:
     *   dur = The `Duration` to add or subtract.
     * Returns:
     *   A new `ParallaxDateTime` object representing the result.
     */
    ParallaxDateTime opBinary(string op)(Duration dur) const
    if (op == "+" || op == "-")
    {
        static if (op == "+")
            return ParallaxDateTime(timestamp + dur);
        else
            return ParallaxDateTime(timestamp - dur);
    }

    /**
     * Calculates the duration between two ParallaxDateTime objects.
     * Params:
     *   other = The other `ParallaxDateTime` object.
     * Returns:
     *   A `Duration` representing the difference.
     */
    Duration opBinary(string op)(ParallaxDateTime other) const if (op == "-") =>
        timestamp - other.timestamp;

    /**
     * Compares two ParallaxDateTime objects.
     * Params:
     *   other = The other ParallaxDateTime object.
     * Returns:
     *   -1 if this is less than other, 0 if equal, 1 if greater.
     */
    int opCmp(const ParallaxDateTime other) const
    {
        if (timestamp < other.timestamp)
            return -1;
        if (timestamp > other.timestamp)
            return 1;
        return 0;
    }

    /**
     * Checks if two ParallaxDateTime objects are equal.
     *
     * Params:
     *   other = The other `ParallaxDateTime` object.
     *
     * Returns:
     *   true if the timestamps are equal, false otherwise.
     */
    bool opEquals(const ParallaxDateTime other) const => timestamp == other.timestamp;

    /**
     * Returns a hash string representation of the ParallaxDateTime.
     * Returns:
     *   A string suitable for hashing.
     */
    string toHash() const => this.toString();

    /**
     * Return an ISO 8601 extended string representation of the ParallaxDateTime.
     *
     * Returns:
     *   An ISO 8601 extended string.
     */
    string toString() const => timestamp.toISOExtString();

    /**
     * Format the ParallaxDateTime according to a format string.
     *
     * Params:
     *   fmt = The format string. Supported specifiers: %Y (year), %m (month), %d (day), %H (hour), %M (minute), %S (second), %A (day name), %B (month name).
     *
     * Returns:
     *   The formatted datetime string.
     */
    string strftime(string fmt) const
    {
        auto result = fmt;
        result = result.replace("%Y", format("%04d", year));
        result = result.replace("%m", format("%02d", month));
        result = result.replace("%d", format("%02d", day));
        result = result.replace("%H", format("%02d", hour));
        result = result.replace("%M", format("%02d", minute));
        result = result.replace("%S", format("%02d", second));
        result = result.replace("%A", dayName);
        result = result.replace("%B", monthName);
        return result;
    }

    /**
     * Floor the ParallaxDateTime to the nearest specified frequency.
     * Params:
     *   freq = The frequency to floor to (e.g., "day", "hour", "minute", "second").
     * Returns:
     *   A new `ParallaxDateTime` object floored to the specified frequency.
     */
    ParallaxDateTime floor(string freq) const
    {
        auto dt = DateTime(
            timestamp.year,
            timestamp.month,
            timestamp.day,
            timestamp.hour,
            timestamp.minute,
            timestamp.second
        );
        switch (freq.toLower)
        {
        case "d":
        case "day":
            return ParallaxDateTime(DateTime(dt.date, TimeOfDay(0, 0, 0)));
        case "h":
        case "hour":
            return ParallaxDateTime(DateTime(dt.date, TimeOfDay(dt.hour, 0, 0)));
        case "min":
        case "minute":
            return ParallaxDateTime(DateTime(dt.date, TimeOfDay(dt.hour, dt.minute, 0)));
        case "s":
        case "second":
            return ParallaxDateTime(DateTime(dt.date, TimeOfDay(dt.hour, dt.minute, dt.second)));
        default:
            return this;
        }
    }

    /**
     * Ceils the ParallaxDateTime to the nearest specified frequency.
     *
     * Params:
     *   freq = The frequency to ceil to (e.g., "day", "hour", "minute", "second").
     * Returns:
     *   A new `ParallaxDateTime` object ceiled to the specified frequency.
     */
    ParallaxDateTime ceil(string freq) const
    {
        auto floored = floor(freq);
        if (floored == this)
            return this;

        switch (freq.toLower)
        {
        case "d":
        case "day":
            return floored + 1.days;
        case "h":
        case "hour":
            return floored + 1.hours;
        case "min":
        case "minute":
            return floored + 1.minutes;
        case "s":
        case "second":
            return floored + 1.seconds;
        default:
            return this;
        }
    }
}

/**
 * Parses a date and time string into a ParallaxDateTime object.
 * It attempts to auto-detect the format if no format string is provided.
 *
 * Params:
 *   dateStr = The date and time string to parse.
 *   format = Optional. The format string to use for parsing. If empty, auto-detection is used.
 *
 * Returns:
 *   A ParallaxDateTime object.
 */
ParallaxDateTime parseDateTime(string dateStr, string format = "")
{
    dateStr = dateStr.strip();

    if (format.length == 0)
    {
        return autoParseDateTime(dateStr);
    }
    else
    {
        return parseWithFormat(dateStr, format);
    }
}

private ParallaxDateTime autoParseDateTime(string dateStr)
{
    auto isoRegex = regex(
        r"^^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:Z|[+-]\d{2}:\d{2})?)?$");
    auto match = matchFirst(dateStr, isoRegex);
    if (!match.empty)
    {
        int year = to!int(match[1]);
        int month = to!int(match[2]);
        int day = to!int(match[3]);
        int hour = match[4].empty ? 0 : to!int(match[4]);
        int minute = match[5].empty ? 0 : to!int(match[5]);
        int second = match[6].empty ? 0 : to!int(match[6]);
        int msecs = match[7].empty ? 0 : to!int(match[7].leftJustify(3, '0')[0 .. 3]);

        return ParallaxDateTime(year, month, day, hour, minute, second, msecs);
    }

    auto usRegex = regex(
        r"^^(\d{1,2})/(\d{1,2})/(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s*(AM|PM))?)?$", "i");
    match = matchFirst(dateStr, usRegex);
    if (!match.empty)
    {
        int month = to!int(match[1]);
        int day = to!int(match[2]);
        int year = to!int(match[3]);
        if (year < 100)
            year += (year > 50) ? 1900 : 2000;

        int hour = match[4].empty ? 0 : to!int(match[4]);
        int minute = match[5].empty ? 0 : to!int(match[5]);
        int second = match[6].empty ? 0 : to!int(match[6]);

        if (!match[7].empty && match[7].toLower == "pm" && hour != 12)
            hour += 12;
        if (!match[7].empty && match[7].toLower == "am" && hour == 12)
            hour = 0;

        return ParallaxDateTime(year, month, day, hour, minute, second);
    }

    auto euRegex = regex(
        r"^^(\d{1,2})[./](\d{1,2})[./](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$");
    match = matchFirst(dateStr, euRegex);
    if (!match.empty)
    {
        int day = to!int(match[1]);
        int month = to!int(match[2]);
        int year = to!int(match[3]);
        if (year < 100)
            year += (year > 50) ? 1900 : 2000;

        int hour = match[4].empty ? 0 : to!int(match[4]);
        int minute = match[5].empty ? 0 : to!int(match[5]);
        int second = match[6].empty ? 0 : to!int(match[6]);

        return ParallaxDateTime(year, month, day, hour, minute, second);
    }

    throw new Exception("Could not parse datetime: " ~ dateStr);
}

/**
 * Parses a date and time string using a specified format.
 * Params:
 *   dateStr = The date and time string to parse.
 *   format = The format string to use for parsing.
 * Returns:
 *   A `ParallaxDateTime` object.
 * Throws:
 *   `Exception` if the string does not match the format.
 */
private ParallaxDateTime parseWithFormat(string dateStr, string format)
{
    string pattern = format;
    pattern = pattern.replace("%Y", r"(\d{4})");
    pattern = pattern.replace("%m", r"(\d{1,2})");
    pattern = pattern.replace("%d", r"(\d{1,2})");
    pattern = pattern.replace("%H", r"(\d{1,2})");
    pattern = pattern.replace("%M", r"(\d{1,2})");
    pattern = pattern.replace("%S", r"(\d{1,2})");

    auto match = matchFirst(dateStr, regex(pattern));
    if (match.empty)
        throw new Exception("Date string does not match format");

    int year = 1970, month = 1, day = 1, hour = 0, minute = 0, second = 0;

    int captureIndex = 1;
    foreach (i, char c; format)
    {
        if (c == '%' && i + 1 < format.length)
        {
            switch (format[i + 1])
            {
            case 'Y':
                year = to!int(match[captureIndex++]);
                break;
            case 'm':
                month = to!int(match[captureIndex++]);
                break;
            case 'd':
                day = to!int(match[captureIndex++]);
                break;
            case 'H':
                hour = to!int(match[captureIndex++]);
                break;
            case 'M':
                minute = to!int(match[captureIndex++]);
                break;
            case 'S':
                second = to!int(match[captureIndex++]);
                break;
            default:
                break;
            }
        }
    }

    return ParallaxDateTime(year, month, day, hour, minute, second);
}

/** 
 * A time series column.
 */
class DateTimeColumn : IColumn
{
    private ParallaxDateTime[] data_;
    private string name_;

    this(string name, const ParallaxDateTime[] data = [])
    {
        name_ = name;
        data_ = data.dup;
    }

    @property string name() const
    {
        return name_;
    }

    @property size_t length() const
    {
        return data_.length;
    }

    IColumn copyWithName(string newName)
    {
        return new DateTimeColumn(newName, data_);
    }

    void append(ParallaxDateTime value)
    {
        data_ ~= value;
    }

    DataValue getValue(size_t index) const
    {
        if (index >= data_.length)
            throw new Exception("Index out of bounds");
        return DataValue(data_[index]);
    }

    void setValue(size_t index, DataValue value)
    {
        if (index >= data_.length)
            throw new Exception("Index out of bounds");

        if (value.convertsTo!ParallaxDateTime)
            data_[index] = value.get!ParallaxDateTime;
        else if (value.convertsTo!string)
            data_[index] = parseDateTime(value.get!string);
        else
            throw new Exception("Cannot convert value to datetime");
    }

    string toString(size_t index) const
    {
        if (index >= data_.length)
            return "";
        return data_[index].toString();
    }

    IColumn copy() const
    {
        return new DateTimeColumn(name_, data_);
    }

    IColumn slice(size_t start, size_t end) const
    {
        if (end > data_.length)
            end = data_.length;
        if (start > end)
            start = end;
        return new DateTimeColumn(name_, data_[start .. end]);
    }

    TCol!int dt_year() const
    {
        auto result = new TCol!int(name_ ~ "_year");
        foreach (dt; data_)
        {
            result.append(dt.year);
        }
        return result;
    }

    TCol!int dt_month() const
    {
        auto result = new TCol!int(name_ ~ "_month");
        foreach (dt; data_)
        {
            result.append(dt.month);
        }
        return result;
    }

    TCol!int dt_day() const
    {
        auto result = new TCol!int(name_ ~ "_day");
        foreach (dt; data_)
        {
            result.append(dt.day);
        }
        return result;
    }

    TCol!int dt_dayofweek() const
    {
        auto result = new TCol!int(name_ ~ "_dayofweek");
        foreach (dt; data_)
        {
            result.append(dt.dayOfWeek);
        }
        return result;
    }

    TCol!string dt_strftime(string format) const
    {
        auto result = new TCol!string(name_ ~ "_formatted");
        foreach (dt; data_)
        {
            result.append(dt.strftime(format));
        }
        return result;
    }

    IColumn reorder(size_t[] indices)
    {
        ParallaxDateTime[] reorderedData;
        reorderedData.reserve(indices.length);

        foreach (idx; indices)
        {
            reorderedData ~= data_[idx];
        }

        return new DateTimeColumn(name_, reorderedData);
    }

    /**
    * Returns actual DateTimeColumn since it preserves DateTime type.
    */
    DateTimeColumn dt_floor(string freq) const
    {
        auto result = new DateTimeColumn(name_ ~ "_floor");
        foreach (dt; data_)
        {
            result.append(dt.floor(freq));
        }
        return result;
    }

    DateTimeColumn dt_ceil(string freq) const
    {
        auto result = new DateTimeColumn(name_ ~ "_ceil");
        foreach (dt; data_)
        {
            result.append(dt.ceil(freq));
        }
        return result;
    }

    ParallaxDateTime[] getData() const
    {
        return data_.dup;
    }

    IColumn createEmpty()
    {
        return new DateTimeColumn(name_, []);
    }

    IColumn filter(bool[] mask)
    {
        import std.exception;

        enforce(mask.length == data_.length, "Mask length must match column length");

        ParallaxDateTime[] filteredData;

        size_t trueCount = 0;
        foreach (val; mask)
        {
            if (val)
                trueCount++;
        }
        filteredData.reserve(trueCount);

        foreach (i, include; mask)
        {
            if (include)
            {
                filteredData ~= data_[i];
            }
        }

        return new DateTimeColumn(name_, filteredData);
    }

}

ParallaxDateTime[] dateRange(ParallaxDateTime start, ParallaxDateTime end, Duration freq)
{
    ParallaxDateTime[] result;
    auto current = start;

    while (current <= end)
    {
        result ~= current;
        current = current + freq;
    }

    return result;
}

ParallaxDateTime[] dateRange(string start, string end, Duration freq) => dateRange(
    parseDateTime(start),
    parseDateTime(end),
    freq
);

ParallaxDateTime[] dateRange(ParallaxDateTime start, int periods, Duration freq)
{
    ParallaxDateTime[] result;
    auto current = start;

    foreach (i; 0 .. periods)
    {
        result ~= current;
        current = current + freq;
    }

    return result;
}

class TimeDelta
{
    Duration duration;

    this(Duration dur)
    {
        duration = dur;
    }

    this(long days = 0, long hours = 0, long minutes = 0, long seconds = 0, long milliseconds = 0)
    {
        duration = days.days + hours.hours + minutes.minutes + seconds.seconds + milliseconds.msecs;
    }

    @property long totalDays() const
    {
        return duration.total!"days";
    }

    @property long totalHours() const
    {
        return duration.total!"hours";
    }

    @property long totalMinutes() const
    {
        return duration.total!"minutes";
    }

    @property long totalSeconds() const
    {
        return duration.total!"seconds";
    }

    @property long totalMilliseconds() const
    {
        return duration.total!"msecs";
    }

    override string toString() const
    {
        auto days = totalDays;
        auto hours = (duration - days.days).total!"hours";
        auto minutes = (duration - days.days - hours.hours).total!"minutes";
        auto seconds = (duration - days.days - hours.hours - minutes.minutes).total!"seconds";

        return format("%d days %02d:%02d:%02d", days, hours, minutes, seconds);
    }
}

ParallaxDateTime now()
{
    return ParallaxDateTime(Clock.currTime);
}

ParallaxDateTime today()
{
    auto now = Clock.currTime;
    return ParallaxDateTime(DateTime(now.year, now.month, now.day));
}

bool isBusinessDay(ParallaxDateTime dt)
{
    auto dow = dt.dayOfWeek;
    return dow != DayOfWeek.sat && dow != DayOfWeek.sun;
}

ParallaxDateTime[] businessDayRange(ParallaxDateTime start, ParallaxDateTime end)
{
    ParallaxDateTime[] result;
    auto current = start;

    while (current <= end)
    {
        if (isBusinessDay(current))
            result ~= current;
        current = current + 1.days;
    }

    return result;
}

ParallaxDateTime toTimezone(ParallaxDateTime dt, immutable TimeZone tz)
{
    return ParallaxDateTime(dt.timestamp.toOtherTZ(tz));
}

struct Period
{
    ParallaxDateTime start;
    ParallaxDateTime end;

    this(ParallaxDateTime s, ParallaxDateTime e)
    {
        start = s;
        end = e;
    }

    @property Duration length() const
    {
        return end - start;
    }

    bool contains(ParallaxDateTime dt) const
    {
        return dt >= start && dt <= end;
    }
}

Period[] splitByFrequency(ParallaxDateTime start, ParallaxDateTime end, string freq)
{
    Period[] periods;
    auto current = start;

    while (current < end)
    {
        ParallaxDateTime next;
        switch (freq.toLower)
        {
        case "d":
        case "day":
            next = current + 1.days;
            break;
        case "w":
        case "week":
            next = current + 7.days;
            break;
        case "m":
        case "month":
            auto ts = current.timestamp;
            auto nextMonth = ts.month == Month.dec ? Month.jan : cast(Month)(ts.month + 1);
            auto nextYear = ts.month == Month.dec ? ts.year + 1 : ts.year;
            next = ParallaxDateTime(nextYear, cast(int) nextMonth, ts.day);
            break;
        case "y":
        case "year":
            next = ParallaxDateTime(current.year + 1, current.month, current.day);
            break;
        default:
            next = current + 1.days;
        }

        if (next > end)
            next = end;
        periods ~= Period(current, next);
        current = next;
    }

    return periods;
}
