using System.Globalization;

namespace NymBroker.Core.Scheduling;

/// <summary>
/// A parsed 5-field cron expression (<c>minute hour day-of-month month day-of-week</c>) with a
/// next-occurrence calculator. Compatible with the Cronos "Standard" format: day-of-month and
/// day-of-week must both match, and daylight saving time is handled the way Cronos handles it.
/// </summary>
internal sealed class CronSchedule
{
    private const int MaxYear = 2099;
    private const int ProbeLimitMinutes = 24 * 60;

    private static readonly string[] MonthNames =
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

    private static readonly string[] DayNames = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

    private readonly string _expression;
    private readonly ulong _minutes;
    private readonly uint _hours;
    private readonly uint _days;          // bit n = day-of-month n
    private readonly uint _lastOffsets;   // bit n = "L-n" (bit 0 = "L")
    private readonly uint _nearest;       // bit n = "nW"
    private readonly uint _lastNearest;   // bit n = "L-nW" (bit 0 = "LW")
    private readonly ushort _months;      // bit n = month n
    private readonly byte _weekdays;      // bit n = weekday n (0 = Sunday)
    private readonly byte _lastWeekdayOfMonth; // "nL"
    private readonly ulong _nth;          // bit (weekday * 5 + k - 1) = "weekday#k"
    private readonly bool _interval;      // minute or hour field uses '*', a range or a step

    private CronSchedule(string expression, string[] f)
    {
        _expression = expression;
        _minutes = ParseSimple(f[0], "minute", 0, 59, null, out var minuteInterval);
        _hours = (uint)ParseSimple(f[1], "hour", 0, 23, null, out var hourInterval);
        _interval = minuteInterval || hourInterval;

        ParseDayOfMonth(f[2], out _days, out _lastOffsets, out _nearest, out _lastNearest);
        _months = (ushort)ParseSimple(f[3], "month", 1, 12, MonthNames, out _, 1);
        ParseDayOfWeek(f[4], out _weekdays, out _lastWeekdayOfMonth, out _nth);
    }

    public static CronSchedule Parse(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            throw new ArgumentException("Cron expression must not be null, empty or whitespace.", nameof(expression));

        var trimmed = expression.Trim();
        var normalized = trimmed.StartsWith('@') ? Macro(trimmed) : trimmed;
        var fields = normalized.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (fields.Length != 5)
            throw new CronFormatException(
                $"Invalid cron expression '{expression}': expected 5 fields (minute hour day-of-month month day-of-week) but found {fields.Length}.");

        try { return new CronSchedule(expression, fields); }
        catch (CronFormatException ex) { throw new CronFormatException($"Invalid cron expression '{expression}': {ex.Message}"); }
    }

    public override string ToString() => _expression;

    /// <summary>First occurrence strictly after <paramref name="from"/> in <paramref name="zone"/>, or null.</summary>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset from, TimeZoneInfo zone)
    {
        var local = TimeZoneInfo.ConvertTime(from, zone);
        var start = Truncate(local.DateTime).AddMinutes(1);

        if (zone.IsAmbiguousTime(local))
        {
            var offsets = zone.GetAmbiguousTimeOffsets(local);
            var standard = offsets.Min();
            var daylight = offsets.Max();
            var (ambiguousStart, ambiguousEnd) = AmbiguousInterval(zone, Truncate(local.DateTime));

            if (local.Offset == daylight)
            {
                var early = FindNext(start);
                if (early is { } e && e < ambiguousEnd) return new DateTimeOffset(e, daylight);
                start = ambiguousStart;
            }

            if (_interval)
            {
                var late = FindNext(start);
                if (late is { } l && l < ambiguousEnd) return new DateTimeOffset(l, standard);
            }

            start = ambiguousEnd;
        }

        var found = FindNext(start);
        return found is { } f ? ToInstant(f, zone) : null;
    }

    private static DateTimeOffset ToInstant(DateTime local, TimeZoneInfo zone)
    {
        if (zone.IsInvalidTime(local))
        {
            var end = FirstWhere(local, 1, t => !zone.IsInvalidTime(t));
            return new DateTimeOffset(end, zone.GetUtcOffset(end));
        }

        if (zone.IsAmbiguousTime(local))
            return new DateTimeOffset(local, zone.GetAmbiguousTimeOffsets(local).Max());

        return new DateTimeOffset(local, zone.GetUtcOffset(local));
    }

    private static (DateTime Start, DateTime End) AmbiguousInterval(TimeZoneInfo zone, DateTime inside)
    {
        var end = FirstWhere(inside, 1, t => !zone.IsAmbiguousTime(t));
        var before = FirstWhere(inside, -1, t => !zone.IsAmbiguousTime(t));
        return (before.AddMinutes(1), end);
    }

    // Smallest whole-minute distance (in `direction`) from `origin` at which `predicate` holds; binary search.
    private static DateTime FirstWhere(DateTime origin, int direction, Func<DateTime, bool> predicate)
    {
        var lo = 0;
        var hi = ProbeLimitMinutes;
        while (hi - lo > 1)
        {
            var mid = (lo + hi) / 2;
            if (predicate(origin.AddMinutes(direction * mid))) hi = mid; else lo = mid;
        }
        return origin.AddMinutes(direction * hi);
    }

    private static DateTime Truncate(DateTime t) => new(t.Ticks - t.Ticks % TimeSpan.TicksPerMinute, DateTimeKind.Unspecified);

    // First matching local wall-clock minute at or after `start`.
    private DateTime? FindNext(DateTime start)
    {
        int year = start.Year, month = start.Month, day = start.Day, hour = start.Hour, minute = start.Minute;

        while (year <= MaxYear)
        {
            var m = NextBit(_months, month, 12);
            if (m < 0) { year++; month = 1; day = 1; hour = 0; minute = 0; continue; }
            if (m != month) { month = m; day = 1; hour = 0; minute = 0; }

            var dayMask = day > 31 ? 0u : MonthDays(year, month) & (uint.MaxValue << day);
            if (dayMask == 0)
            {
                if (++month > 12) { month = 1; year++; }
                day = 1; hour = 0; minute = 0;
                continue;
            }
            var d = System.Numerics.BitOperations.TrailingZeroCount(dayMask);
            if (d != day) { day = d; hour = 0; minute = 0; }

            var h = NextBit(_hours, hour, 23);
            if (h < 0) { day++; hour = 0; minute = 0; continue; }
            if (h != hour) { hour = h; minute = 0; }

            var mi = NextBit(_minutes, minute, 59);
            if (mi < 0) { hour++; minute = 0; continue; }

            return new DateTime(year, month, day, hour, mi, 0, DateTimeKind.Unspecified);
        }

        return null;
    }

    private static int NextBit(ulong mask, int from, int max)
    {
        if (from > max) return -1;
        var rest = mask >> from << from;
        return rest == 0 ? -1 : System.Numerics.BitOperations.TrailingZeroCount(rest);
    }

    // Bit n set when day n of the month matches both day-of-month and day-of-week.
    private uint MonthDays(int year, int month)
    {
        var dim = DateTime.DaysInMonth(year, month);
        var firstWeekday = (int)new DateTime(year, month, 1).DayOfWeek;
        var all = (uint)((1UL << (dim + 1)) - 2);

        var dom = _days & all;
        for (var bits = _lastOffsets; bits != 0; bits &= bits - 1)
        {
            var d = dim - System.Numerics.BitOperations.TrailingZeroCount(bits);
            if (d >= 1) dom |= 1u << d;
        }
        for (var bits = _nearest; bits != 0; bits &= bits - 1)
        {
            var n = System.Numerics.BitOperations.TrailingZeroCount(bits);
            if (n <= dim) dom |= 1u << NearestWeekday(n, dim, firstWeekday);
        }
        for (var bits = _lastNearest; bits != 0; bits &= bits - 1)
        {
            var d = dim - System.Numerics.BitOperations.TrailingZeroCount(bits);
            if (d >= 1) dom |= 1u << NearestWeekday(d, dim, firstWeekday);
        }

        if (_weekdays == 0x7F && _lastWeekdayOfMonth == 0 && _nth == 0) return dom;

        uint dow = 0;
        for (var d = 1; d <= dim; d++)
        {
            var wd = (firstWeekday + d - 1) % 7;
            if ((_weekdays >> wd & 1) != 0
                || (_lastWeekdayOfMonth >> wd & 1) != 0 && d + 7 > dim
                || (_nth >> (wd * 5 + (d - 1) / 7) & 1) != 0)
                dow |= 1u << d;
        }
        return dom & dow;
    }

    private static int NearestWeekday(int day, int dim, int firstWeekday)
    {
        var wd = (firstWeekday + day - 1) % 7;
        if (wd == 6) return day == 1 ? 3 : day - 1;
        if (wd == 0) return day == dim ? day - 2 : day + 1;
        return day;
    }

    // ---- parsing ----

    private static string Macro(string macro) => macro.ToLowerInvariant() switch
    {
        "@yearly" or "@annually" => "0 0 1 1 *",
        "@monthly" => "0 0 1 * *",
        "@weekly" => "0 0 * * 0",
        "@daily" or "@midnight" => "0 0 * * *",
        "@hourly" => "0 * * * *",
        _ => throw new CronFormatException($"Invalid cron expression '{macro}': unknown macro '{macro}'."),
    };

    private static void ParseDayOfMonth(string token, out uint days, out uint lastOffsets, out uint nearest, out uint lastNearest)
    {
        days = 0; lastOffsets = 0; nearest = 0; lastNearest = 0;

        if (token == "?") { days = AllDays; return; }

        var upper = token.ToUpperInvariant();
        var weekday = upper.Length > 1 && upper.EndsWith('W') && !upper.Contains(',');
        var body = weekday ? upper[..^1] : upper;

        if (body == "L" || body.StartsWith("L-", StringComparison.Ordinal))
        {
            var offset = body == "L" ? 0 : Number(body[2..], "day-of-month", token, 1, 30);
            if (weekday) lastNearest = 1u << offset; else lastOffsets = 1u << offset;
            return;
        }

        if (weekday)
        {
            nearest = 1u << Number(body, "day-of-month", token, 1, 31);
            return;
        }

        days = (uint)ParseSimple(token, "day-of-month", 1, 31, null, out _);
    }

    private const uint AllDays = 0xFFFFFFFEu;

    private static void ParseDayOfWeek(string token, out byte weekdays, out byte lastOfMonth, out ulong nth)
    {
        weekdays = 0; lastOfMonth = 0; nth = 0;

        if (token == "?") { weekdays = 0x7F; return; }

        var hash = token.IndexOf('#');
        if (hash >= 0)
        {
            var wd = DayValue(token[..hash], token) % 7;
            var k = Number(token[(hash + 1)..], "day-of-week", token, 1, 5);
            nth = 1UL << (wd * 5 + k - 1);
            return;
        }

        if (token.Length > 1 && token.EndsWith("L", StringComparison.OrdinalIgnoreCase) && !token.Contains(','))
        {
            lastOfMonth = (byte)(1 << DayValue(token[..^1], token) % 7);
            return;
        }

        var bits = ParseSimple(token, "day-of-week", 0, 7, DayNames, out _);
        if ((bits & 0x80) != 0) bits |= 1;
        weekdays = (byte)(bits & 0x7F);
    }

    private static int DayValue(string text, string token)
    {
        var index = Array.FindIndex(DayNames, n => n.Equals(text, StringComparison.OrdinalIgnoreCase));
        return index >= 0 ? index : Number(text, "day-of-week", token, 0, 7);
    }

    // Comma-separated list of: *, a, a-b, optionally followed by /step. `nameBase` is the value of the first name.
    private static ulong ParseSimple(string token, string field, int min, int max, string[]? names, out bool interval, int nameBase = 0)
    {
        interval = false;
        ulong mask = 0;

        foreach (var item in token.Split(','))
        {
            if (item.Length == 0) throw Bad(field, $"empty list item in '{token}'");

            var slash = item.IndexOf('/');
            var range = slash >= 0 ? item[..slash] : item;
            var step = 1;
            if (slash >= 0)
            {
                step = Number(item[(slash + 1)..], field, item, 1, max, "step");
                interval = true;
            }

            int lo, hi;
            if (range == "*")
            {
                (lo, hi) = (min, field == "day-of-week" ? 6 : max);
                interval = true;
            }
            else
            {
                var dash = range.IndexOf('-');
                if (dash >= 0)
                {
                    lo = Value(range[..dash], field, min, max, names, nameBase);
                    hi = Value(range[(dash + 1)..], field, min, max, names, nameBase);
                    if (lo > hi) throw Bad(field, $"range {range} is reversed");
                    interval = true;
                }
                else
                {
                    lo = Value(range, field, min, max, names, nameBase);
                    hi = slash >= 0 ? max : lo;
                }
            }

            for (var v = lo; v <= hi; v += step) mask |= 1UL << v;
        }

        return mask;
    }

    private static int Value(string text, string field, int min, int max, string[]? names, int nameBase)
    {
        if (names != null)
        {
            var index = Array.FindIndex(names, n => n.Equals(text, StringComparison.OrdinalIgnoreCase));
            if (index >= 0) return index + nameBase;
        }
        return Number(text, field, text, min, max);
    }

    private static int Number(string text, string field, string token, int min, int max, string what = "value")
    {
        if (text.Length == 0 || !int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            throw Bad(field, $"invalid {what} '{text}' in '{token}'");
        if (value < min || value > max)
            throw Bad(field, $"{what} {value} is out of range {min}-{max}");
        return value;
    }

    private static CronFormatException Bad(string field, string message) => new($"{field} {message}.");
}
