using CronosExpression = Cronos.CronExpression;
using NymBroker.Core.Scheduling;

namespace NymBroker.Tests;

public class CronScheduleTests
{
    private static readonly DateTimeOffset Jan2024 = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static DateTimeOffset Utc(int y, int mo, int d, int h = 0, int mi = 0, int s = 0)
        => new(y, mo, d, h, mi, s, TimeSpan.Zero);

    // Europe/Stockholm-like: +1, DST +1 from the last Sunday in March 02:00 until the last Sunday in October 03:00.
    private static readonly TimeZoneInfo Stockholm = CustomZone("Stockholm", TimeSpan.FromHours(1),
        TimeZoneInfo.TransitionTime.CreateFloatingDateRule(new DateTime(1, 1, 1, 2, 0, 0), 3, 5, DayOfWeek.Sunday),
        TimeZoneInfo.TransitionTime.CreateFloatingDateRule(new DateTime(1, 1, 1, 3, 0, 0), 10, 5, DayOfWeek.Sunday));

    // America/New_York-like: -5, DST from the second Sunday in March 02:00 until the first Sunday in November 02:00.
    private static readonly TimeZoneInfo NewYork = CustomZone("NewYork", TimeSpan.FromHours(-5),
        TimeZoneInfo.TransitionTime.CreateFloatingDateRule(new DateTime(1, 1, 1, 2, 0, 0), 3, 2, DayOfWeek.Sunday),
        TimeZoneInfo.TransitionTime.CreateFloatingDateRule(new DateTime(1, 1, 1, 2, 0, 0), 11, 1, DayOfWeek.Sunday));

    private static TimeZoneInfo CustomZone(string id, TimeSpan baseOffset,
        TimeZoneInfo.TransitionTime start, TimeZoneInfo.TransitionTime end)
    {
        var rule = TimeZoneInfo.AdjustmentRule.CreateAdjustmentRule(
            DateTime.MinValue.Date, DateTime.MaxValue.Date, TimeSpan.FromHours(1), start, end);
        return TimeZoneInfo.CreateCustomTimeZone(id, baseOffset, id, id, id + " DST", [rule]);
    }

    // ---- parsing ----

    [Theory]
    [InlineData("* * * * *")]
    [InlineData("0 0 1 1 *")]
    [InlineData("59 23 31 12 7")]
    [InlineData("*/15 */2 * * *")]
    [InlineData("10-20/5 8-18 * * *")]
    [InlineData("5/10 3/4 * * *")]
    [InlineData("0,15,30,45 0,12 * * *")]
    [InlineData("0 0 ? * ?")]
    [InlineData("0 0 L * *")]
    [InlineData("0 0 L-2 * *")]
    [InlineData("0 0 15W * *")]
    [InlineData("0 0 LW * *")]
    [InlineData("0 0 L-3W * *")]
    [InlineData("0 0 * jan,Feb,MAR-may *")]
    [InlineData("0 0 * * Mon-Fri")]
    [InlineData("0 0 * * SUN,sat")]
    [InlineData("0 0 * * 0,7")]
    [InlineData("0 0 * * 5L")]
    [InlineData("0 0 * * 1#3")]
    [InlineData("0 0 * * FRI#2")]
    [InlineData("0 0 * * SUNL")]
    [InlineData("  0 \t 0   *  * \t * ")]
    [InlineData("@yearly")]
    [InlineData("@annually")]
    [InlineData("@monthly")]
    [InlineData("@weekly")]
    [InlineData("@daily")]
    [InlineData("@midnight")]
    [InlineData("@hourly")]
    [InlineData("@DAILY")]
    public void Parse_ValidExpressions(string expression)
    {
        var schedule = CronSchedule.Parse(expression);
        Assert.Equal(expression, schedule.ToString());
    }

    [Theory]
    [InlineData("* * * *")]
    [InlineData("* * * * * *")]
    [InlineData("60 * * * *")]
    [InlineData("* 24 * * *")]
    [InlineData("* * 0 * *")]
    [InlineData("* * 32 * *")]
    [InlineData("* * * 13 *")]
    [InlineData("* * * 0 *")]
    [InlineData("* * * * 8")]
    [InlineData("5-1 * * * *")]
    [InlineData("*/0 * * * *")]
    [InlineData("*/x * * * *")]
    [InlineData("* * * FOO *")]
    [InlineData("* * * * FOO")]
    [InlineData("@sometimes")]
    [InlineData("L * * * *")]
    [InlineData("* 5W * * *")]
    [InlineData("* * * L *")]
    [InlineData("* * * * L")]
    [InlineData("* * * * 1#6")]
    [InlineData("* * * * 1#0")]
    [InlineData("* * 5# * *")]
    [InlineData("* * L,5 * *")]
    [InlineData("* * 0W * *")]
    [InlineData("* * * * 1,2L")]
    [InlineData("1,,2 * * * *")]
    public void Parse_InvalidExpressions_ThrowCronFormatException(string expression)
        => Assert.IsAssignableFrom<FormatException>(Assert.Throws<CronFormatException>(() => CronSchedule.Parse(expression)));

    [Fact]
    public void Parse_OutOfRange_MessageNamesFieldAndValue()
    {
        var ex = Assert.Throws<CronFormatException>(() => CronSchedule.Parse("0 25 * * *"));
        Assert.Equal("Invalid cron expression '0 25 * * *': hour value 25 is out of range 0-23.", ex.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_NullOrWhitespace_ThrowsArgumentException(string? expression)
        => Assert.Throws<ArgumentException>(() => CronSchedule.Parse(expression!));

    // ---- known answers (UTC) ----

    [Theory]
    [InlineData("* * * * *", "2024-03-05T10:20:30Z", "2024-03-05T10:21:00Z")]
    [InlineData("0 17 * * 1-5", "2024-03-01T18:00:00Z", "2024-03-04T17:00:00Z")] // Friday evening -> Monday
    [InlineData("0 0 1 * *", "2024-12-31T12:00:00Z", "2025-01-01T00:00:00Z")]
    [InlineData("59 23 31 12 *", "2024-12-31T23:59:00Z", "2025-12-31T23:59:00Z")]
    [InlineData("0 0 29 2 *", "2027-01-01T00:00:00Z", "2028-02-29T00:00:00Z")]
    [InlineData("0 0 L * *", "2024-02-10T00:00:00Z", "2024-02-29T00:00:00Z")]
    [InlineData("0 0 L-2 * *", "2024-04-01T00:00:00Z", "2024-04-28T00:00:00Z")]
    [InlineData("0 0 15W * *", "2024-06-01T00:00:00Z", "2024-06-14T00:00:00Z")] // 15 June 2024 is a Saturday
    [InlineData("0 0 15W * *", "2024-09-01T00:00:00Z", "2024-09-16T00:00:00Z")] // 15 Sept 2024 is a Sunday
    [InlineData("0 0 1W * *", "2024-06-01T00:00:00Z", "2024-06-03T00:00:00Z")] // 1 June 2024 is a Saturday
    [InlineData("0 0 LW * *", "2024-06-01T00:00:00Z", "2024-06-28T00:00:00Z")] // 30 June 2024 is a Sunday
    [InlineData("0 0 * * 5L", "2024-03-01T00:00:00Z", "2024-03-29T00:00:00Z")]
    [InlineData("0 0 * * 1#3", "2024-03-01T00:00:00Z", "2024-03-18T00:00:00Z")]
    [InlineData("0 0 * * 7", "2024-03-01T00:00:00Z", "2024-03-03T00:00:00Z")]
    [InlineData("0 0 13 * 5", "2024-01-01T00:00:00Z", "2024-09-13T00:00:00Z")] // both fields must match
    [InlineData("0 0 * jan-feb *", "2024-03-01T00:00:00Z", "2025-01-01T00:00:00Z")]
    [InlineData("@hourly", "2024-03-05T10:00:00Z", "2024-03-05T11:00:00Z")] // exclusive of `from`
    [InlineData("*/15 * * * *", "2024-03-05T10:14:59.999Z", "2024-03-05T10:15:00Z")]
    [InlineData("*/15 * * * *", "2024-03-05T10:15:00.001Z", "2024-03-05T10:30:00Z")]
    [InlineData("0 0 30 2 *", "2024-01-01T00:00:00Z", null)]
    public void GetNextOccurrence_KnownAnswers(string expression, string from, string? expected)
    {
        var actual = CronSchedule.Parse(expression).GetNextOccurrence(DateTimeOffset.Parse(from), TimeZoneInfo.Utc);
        Assert.Equal(expected is null ? null : DateTimeOffset.Parse(expected), actual);
    }

    [Fact]
    public void GetNextOccurrence_ResultOffsetIsZoneOffset()
    {
        var next = CronSchedule.Parse("0 12 * * *").GetNextOccurrence(Utc(2024, 7, 1), Stockholm);
        Assert.Equal(new DateTimeOffset(2024, 7, 1, 12, 0, 0, TimeSpan.FromHours(2)), next);
        Assert.Equal(TimeSpan.FromHours(2), next!.Value.Offset);
    }

    // ---- daylight saving time (asserted against Cronos) ----

    [Theory]
    [InlineData("30 2 * * *")]      // spring-forward gap: fires at the transition
    [InlineData("*/30 2 * * *")]
    [InlineData("0 3 * * *")]
    [InlineData("30 2 * * *", true)]
    [InlineData("*/15 * * * *", true)]
    public void Dst_Stockholm_MatchesCronos(string expression, bool fallBack = false)
    {
        var from = fallBack ? Utc(2024, 10, 26, 22) : Utc(2024, 3, 30, 22);
        AssertSameSequence(expression, from, Stockholm, 12);
    }

    [Fact]
    public void Dst_SpringForwardGap_FiresAtTransition()
    {
        var next = CronSchedule.Parse("30 2 * * *").GetNextOccurrence(Utc(2024, 3, 30, 22), Stockholm);
        Assert.Equal(new DateTimeOffset(2024, 3, 31, 3, 0, 0, TimeSpan.FromHours(2)), next);
    }

    [Fact]
    public void Dst_FallBack_FixedTimeFiresOnce_IntervalFiresTwice()
    {
        var from = Utc(2024, 10, 26, 22);
        var fixedTime = CronSchedule.Parse("30 2 * * *");
        var first = fixedTime.GetNextOccurrence(from, Stockholm)!.Value;
        var second = fixedTime.GetNextOccurrence(first, Stockholm)!.Value;
        Assert.Equal(TimeSpan.FromHours(2), first.Offset);
        Assert.Equal(new DateTimeOffset(2024, 10, 28, 2, 30, 0, TimeSpan.FromHours(1)), second);

        var interval = CronSchedule.Parse("30 */1 * * *");
        var a = interval.GetNextOccurrence(Utc(2024, 10, 27, 0, 0), Stockholm)!.Value;   // 02:00 +02 .. 
        Assert.Equal(new DateTimeOffset(2024, 10, 27, 2, 30, 0, TimeSpan.FromHours(2)), a);
        var b = interval.GetNextOccurrence(a, Stockholm)!.Value;
        Assert.Equal(new DateTimeOffset(2024, 10, 27, 2, 30, 0, TimeSpan.FromHours(1)), b);
    }

    private static void AssertSameSequence(string expression, DateTimeOffset from, TimeZoneInfo zone, int count)
    {
        var ours = CronSchedule.Parse(expression);
        var cronos = CronosExpression.Parse(expression);
        var a = from;
        var b = from;
        for (var i = 0; i < count; i++)
        {
            var expected = cronos.GetNextOccurrence(b, zone);
            var actual = ours.GetNextOccurrence(a, zone);
            Assert.Equal(expected, actual);
            Assert.Equal(expected?.Offset, actual?.Offset);
            if (expected is null) return;
            a = actual!.Value;
            b = expected.Value;
        }
    }

    // ---- differential test against Cronos ----

    private const int Seed = 71;

    [Fact]
    public void Differential_AgreesWithCronos()
    {
        var rng = new Random(Seed);
        var zones = new[] { TimeZoneInfo.Utc, Stockholm, NewYork };
        var failures = new List<string>();

        for (var i = 0; i < 5000; i++)
        {
            var expression = RandomExpression(rng);
            var ours = CronSchedule.Parse(expression);
            CronosExpression cronos;
            try { cronos = CronosExpression.Parse(expression); }
            catch (Exception ex) { throw new InvalidOperationException($"Cronos rejected '{expression}'", ex); }

            foreach (var zone in zones)
            {
                for (var j = 0; j < 20; j++)
                {
                    var from = RandomFrom(rng, zone);
                    var expected = cronos.GetNextOccurrence(from, zone);
                    var actual = ours.GetNextOccurrence(from, zone);
                    if (expected != actual || expected?.Offset != actual?.Offset)
                        failures.Add($"seed={Seed} zone={zone.Id} expression='{expression}' from={from:O}: expected {expected:O}, actual {actual:O}");
                }
            }

            if (failures.Count >= 10) break;
        }

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    private static DateTimeOffset RandomFrom(Random rng, TimeZoneInfo zone)
    {
        var year = rng.Next(2024, 2031);
        if (zone != TimeZoneInfo.Utc && rng.Next(2) == 0)
        {
            // Close to one of the zone's transitions, to exercise the gap and the overlap.
            var rules = zone.GetAdjustmentRules()[0];
            var month = rng.Next(2) == 0 ? rules.DaylightTransitionStart.Month : rules.DaylightTransitionEnd.Month;
            var day = rng.Next(1, 29);
            return new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero)
                .AddMinutes(rng.Next(0, 24 * 60 * 3))
                .AddSeconds(rng.Next(60));
        }

        var start = new DateTimeOffset(year, 1, 1, 0, 0, 0, TimeSpan.Zero);
        return start.AddMinutes(rng.Next(0, 365 * 24 * 60)).AddSeconds(rng.Next(60)).AddMilliseconds(rng.Next(1000));
    }

    private static readonly string[] BiasedHours = ["1", "2", "3", "2-3", "1-3", "*", "*/2", "2,3", "0-4/2"];
    private static readonly string[] MonthNameList = ["JAN", "feb", "Mar", "APR", "may", "JUN", "jul", "AUG", "SEP", "Oct", "NOV", "DEC"];
    private static readonly string[] DayNameList = ["SUN", "mon", "Tue", "WED", "thu", "FRI", "sat"];

    private static string RandomExpression(Random rng)
    {
        var minute = Pick(rng, 0, 59, null, allowStar: true);
        var hour = rng.Next(10) < 3 ? BiasedHours[rng.Next(BiasedHours.Length)] : Pick(rng, 0, 23, null, allowStar: true);
        var month = rng.Next(3) == 0 ? Pick(rng, 1, 12, MonthNameList, allowStar: false) : "*";

        string dom = "*", dow = "*";
        switch (rng.Next(10))
        {
            case 0: dom = "?"; break;
            case 1 or 2: dom = Pick(rng, 1, 31, null, allowStar: false); break;
            case 3: dom = "L"; break;
            case 4: dom = "L-" + rng.Next(1, 31); break;
            case 5: dom = rng.Next(1, 32) + "W"; break;
            case 6: dom = rng.Next(2) == 0 ? "LW" : "L-" + rng.Next(1, 31) + "W"; break;
        }
        switch (rng.Next(10))
        {
            case 0: dow = "?"; break;
            case 1 or 2: dow = Pick(rng, 0, 7, DayNameList, allowStar: false); break;
            case 3: dow = DayWord(rng) + "L"; break;
            case 4: dow = DayWord(rng) + "#" + rng.Next(1, 6); break;
        }

        // Keep most expressions satisfiable: a restricted day-of-month rarely combines with a restricted day-of-week.
        if (dom != "*" && dom != "?" && dow != "*" && dow != "?" && rng.Next(4) != 0) dow = "*";

        return $"{minute} {hour} {dom} {month} {dow}";
    }

    private static string DayWord(Random rng) => rng.Next(2) == 0 ? rng.Next(0, 8).ToString() : DayNameList[rng.Next(7)];

    private static string Pick(Random rng, int min, int max, string[]? names, bool allowStar)
    {
        var items = new List<string>();
        var count = rng.Next(1, 4);
        for (var i = 0; i < count; i++)
        {
            string Value(int v)
            {
                var index = v - (names?.Length == 12 ? 1 : 0);
                return names != null && index < names.Length && rng.Next(2) == 0 ? names[index] : v.ToString();
            }
            var kind = rng.Next(allowStar ? 6 : 5);
            var a = rng.Next(min, max + 1);
            var b = rng.Next(a, max + 1);
            var step = rng.Next(1, Math.Max(2, (max - min) / 2 + 1));
            if (kind == 5) return rng.Next(2) == 0 ? "*" : $"*/{step}";
            items.Add(kind switch
            {
                0 or 1 => Value(a),
                2 => $"{Value(a)}-{Value(b)}",
                3 => $"{Value(a)}-{Value(b)}/{step}",
                4 => $"{Value(a)}/{step}",
                _ => string.Empty,
            });
        }
        return string.Join(',', items);
    }
}
