using System.Text;
using System.Text.Json;

namespace NymBroker.Tests.CodeHealth;

/// <summary>
/// Complexity gate for the production code (docs/architecture.md#code-health, issue #112). Each metric has the target
/// bands good / acceptable / warning, and the gate is the top of the acceptable band: nothing in production code may
/// be in the warning band. The only way past it is an entry in <see cref="Exceptions"/> with a reason, and an entry
/// is a ratchet — its ceiling is the method's current value, and it must go once the method is back within the band.
/// </summary>
public sealed class CodeHealthTests
{
    /// <summary>A method metric with its target bands (inclusive upper bounds); the gate is <see cref="Acceptable"/>.</summary>
    internal sealed record MethodMetric(string Name, Func<MethodMetrics, int> Value, int Good, int Acceptable);

    internal static readonly MethodMetric[] MethodMetrics =
    [
        new("Lines", m => m.Lines, Good: 15, Acceptable: 30),
        new("Cyclomatic", m => m.Cyclomatic, Good: 5, Acceptable: 10),
        new("Cognitive", m => m.Cognitive, Good: 5, Acceptable: 15),
        // The method body is level 0, so an `if` in a loop is 2: the target "at most 2 levels".
        new("Nesting", m => m.Nesting, Good: 1, Acceptable: 2),
        // A CancellationToken is not counted (CodeMetrics.ParameterCount); constructors are not gated (DI fills them).
        new("Parameters", m => m.Parameters, Good: 3, Acceptable: 4),
    ];

    /// <summary>Lines of one type declaration (each file of a partial class counts on its own).</summary>
    internal const int TypeGood = 199, TypeAcceptable = 500;

    /// <summary>Production code lines in a block duplicated elsewhere, in percent.</summary>
    internal const double DuplicationAcceptable = 3.0;

    /// <summary>
    /// A method allowed above the acceptable band for one metric. <paramref name="Method"/> is <c>Type.Name</c> (it covers
    /// every overload), <paramref name="Max"/> is the method's current value — the ceiling, lowered as the method
    /// improves — and <paramref name="Reason"/> says why splitting it would make the code worse, not just longer.
    /// </summary>
    internal sealed record CodeHealthException(string Method, string Metric, int Max, string Reason);

    private const string ShippedApi = "Shipped public API: changing the parameter list is a breaking change for every caller. ";

    private const string SqlScript = "One SQL script in a single raw string literal (cyclomatic 1, no logic): the statements " +
        "run together and are read and reviewed against the database as one unit; splitting the text across methods would " +
        "only scatter it.";

    internal static readonly CodeHealthException[] Exceptions =
    [
        new("NymBrokerHealthChecksBuilderExtensions.AddNymBroker", "Parameters", 5,
            ShippedApi + "It mirrors the AddCheck convention of Microsoft.Extensions.Diagnostics.HealthChecks " +
            "(name, failureStatus, tags, timeout), so it reads like every other health-check registration."),
        new("EndpointHandler.InvokeAsync", "Parameters", 5,
            ShippedApi + "Every custom endpoint calls it; the handler and payload run the message, and the logger, " +
            "endpoint name and optional message id are what its error log line needs."),
        new("HealthCheckResult.FromProbeAsync", "Parameters", 5,
            ShippedApi + "Every endpoint health check calls it: kind, name and logger for the log line, the probe, " +
            "and an optional timeout."),
        new("HealthCheckResult.FromProbe", "Parameters", 5,
            ShippedApi + "The blocking form of FromProbeAsync, with the same parameters."),
        new("NymBrokerImpl.PostBatchAsync", "Parameters", 5,
            ShippedApi + "It implements INymBroker.PostBatchAsync, whose optional splitThresholdBytes, compress and " +
            "correlationId match the other post methods."),
        new("PostgresQueueSql.CreateSchema", "Lines", 39, SqlScript),
        new("SqlServerQueueSql.CreateSchema", "Lines", 45, SqlScript),
        new("SqlServerIdempotencySql.CreateSchema", "Lines", 41, SqlScript),
        new("SqlServerOutboxSql.CreateSchema", "Lines", 46, SqlScript),
        new("SqlServerScheduleSql.CreateSchema", "Lines", 32, SqlScript),
        new("SqlServerScheduleSql.Claim", "Lines", 36, SqlScript +
            " It is one atomic claim (update, else insert, with the duplicate-key race handled in T-SQL)."),
    ];

    /// <summary>Set to a file path to write the full JSON report (the code-health skill's summarize.js reads it).</summary>
    private const string ReportVariable = "NYMBROKER_CODE_HEALTH_REPORT";

    /// <summary>Set to a file path to append a Markdown summary (CI passes $GITHUB_STEP_SUMMARY).</summary>
    private const string SummaryVariable = "NYMBROKER_CODE_HEALTH_SUMMARY";

    private static readonly Lazy<CodeHealthReport> Report = new(() => CodeMetrics.Analyze(CodeMetrics.FindRepoRoot()));

    private static IEnumerable<MethodMetrics> ProductionMethods => Report.Value.Methods.Where(m => CodeMetrics.IsProduction(m.Area));

    private static IEnumerable<TypeMetrics> ProductionTypes => Report.Value.Types.Where(t => CodeMetrics.IsProduction(t.Area));

    [Fact]
    public void ProductionMethods_StayWithinTheAcceptableBand()
    {
        WriteRequestedOutputs();

        var violations = ProductionMethods
            .Select(m => (Method: m, Broken: BrokenLimits(m)))
            .Where(x => x.Broken.Length > 0)
            .OrderBy(x => x.Method.Location, StringComparer.Ordinal)
            .Select(x => $"  {x.Method.Location} {Key(x.Method)}: {string.Join(", ", x.Broken)}")
            .ToList();

        Assert.True(violations.Count == 0,
            $"{violations.Count} production method(s) are above the acceptable band " +
            $"({string.Join(", ", MethodMetrics.Select(x => $"{x.Name.ToLowerInvariant()} {x.Acceptable}"))}). " +
            "Split them up (docs/architecture.md#code-health). An exception needs a reason why splitting would make " +
            "the code worse, in CodeHealthTests.Exceptions.\n" + string.Join("\n", violations));
    }

    [Fact]
    public void Exceptions_AreJustified_AndStillNeeded()
    {
        var methods = ProductionMethods.ToList();
        var problems = new List<string>();
        foreach (var exception in Exceptions)
        {
            var metric = MethodMetrics.SingleOrDefault(m => m.Name == exception.Metric);
            var matches = methods.Where(m => Key(m) == exception.Method).ToList();
            if (metric is null)
                problems.Add($"  {exception.Method}: unknown metric '{exception.Metric}'.");
            else if (matches.Count == 0)
                problems.Add($"  {exception.Method}: no such production method; remove the exception.");
            else if (matches.Max(metric.Value) is var worst && worst <= metric.Acceptable)
                problems.Add($"  {exception.Method}: {exception.Metric.ToLowerInvariant()} {worst} is acceptable now; remove the exception.");
            else if (worst < exception.Max)
                problems.Add($"  {exception.Method}: {exception.Metric.ToLowerInvariant()} dropped to {worst}; lower Max from {exception.Max}.");

            if (exception.Reason.Trim().Length < 60)
                problems.Add($"  {exception.Method}: the reason must explain why splitting would make the code worse.");
        }

        var duplicates = Exceptions.GroupBy(e => (e.Method, e.Metric)).Where(g => g.Count() > 1).Select(g => $"  {g.Key}: listed twice.");
        problems.AddRange(duplicates);

        Assert.True(problems.Count == 0, "Code-health exceptions to update:\n" + string.Join("\n", problems));
    }

    [Fact]
    public void ProductionTypes_StayWithinSizeLimit()
    {
        var violations = ProductionTypes.Where(t => t.Lines > TypeAcceptable)
            .Select(t => $"  {t.File} {t.Kind} {t.Name}: {t.Lines} lines")
            .ToList();

        Assert.True(violations.Count == 0,
            $"{violations.Count} production type declaration(s) exceed {TypeAcceptable} lines. Split the responsibilities " +
            "into smaller types (or a partial class into files per concern) — see docs/architecture.md#code-health.\n" +
            string.Join("\n", violations));
    }

    [Fact]
    public void ProductionCode_DuplicationStaysWithinLimit()
    {
        var report = Report.Value;
        Assert.True(report.DuplicationPercent <= DuplicationAcceptable,
            $"{report.DuplicationPercent:F2} % of the production code is duplicated (limit {DuplicationAcceptable} %). " +
            "Move shared code between packages to shared/ — see docs/architecture.md#code-health.\n" +
            string.Join("\n", report.Duplicates.Select(d => "  " + string.Join("  =  ", d.FirstLines))));
    }

    [Fact]
    public void Analyzer_FindsTheProductionCode()
    {
        // Guards the gate against passing vacuously, e.g. if the repository root or the file filter goes wrong.
        var methods = ProductionMethods.ToList();
        Assert.True(methods.Count > 500, $"Only {methods.Count} production methods found.");
        Assert.Contains(methods, m => m is { Type: "NymBrokerImpl", Name: "ProcessMessageAsync" });
        Assert.Contains(methods, m => m.Area == "shared");
        Assert.DoesNotContain(Report.Value.Methods, m => m.File.Contains("/obj/", StringComparison.Ordinal));
        Assert.True(Report.Value.ProductionCodeLines > 5000, $"Only {Report.Value.ProductionCodeLines} production code lines found.");
    }

    [Fact]
    public void Metrics_OfAKnownMethod()
    {
        const string source = """
            class C
            {
                void M(int a, bool b)
                {
                    if (a > 0)
                    {
                        for (var i = 0; i < a; i++)
                        {
                            if (b && a > 1) { }
                        }
                    }
                    else { }
                }
            }
            """;

        var m = Assert.Single(CodeMetrics.MeasureMethods(source));

        Assert.Equal(("C", "M", 3), (m.Type, m.Name, m.Line));
        Assert.Equal(11, m.Lines);
        Assert.Equal(5, m.Cyclomatic);   // 1 + if + for + if + &&
        Assert.Equal(8, m.Cognitive);    // if 1, for 2 (nested), inner if 3, && 1, else 1
        Assert.Equal(3, m.Nesting);
        Assert.Equal(2, m.Parameters);
    }

    [Fact]
    public void Metrics_LocalFunctionsAndElseIf()
    {
        const string source = """
            class C
            {
                int M(int a)
                {
                    if (a == 1) return 1;
                    else if (a == 2) return 2;
                    return Local(a);

                    int Local(int x) => x > 0 ? x : -x;
                }
            }
            """;

        var methods = CodeMetrics.MeasureMethods(source);
        var outer = Assert.Single(methods, m => m.Name == "M");
        var local = Assert.Single(methods, m => m.Name == "local:Local");

        Assert.Equal((3, 2), (outer.Cyclomatic, outer.Cognitive));   // the local function's ?: is not counted here
        Assert.Equal((2, 1), (local.Cyclomatic, local.Cognitive));
    }

    [Fact]
    public void Metrics_DoNotCountACancellationToken()
    {
        const string source = """
            using System.Threading;
            class C
            {
                void M(int a, string b, CancellationToken ct) { }
                void N(int a, System.Threading.CancellationToken cancellationToken = default) { }
            }
            """;

        var methods = CodeMetrics.MeasureMethods(source);

        Assert.Equal(2, Assert.Single(methods, m => m.Name == "M").Parameters);
        Assert.Equal(1, Assert.Single(methods, m => m.Name == "N").Parameters);
    }

    private static string Key(MethodMetrics m) => $"{m.Type}.{m.Name}";

    private static string[] BrokenLimits(MethodMetrics m)
        => MethodMetrics
            .Where(metric => !(metric.Name == "Parameters" && m.Name == ".ctor"))
            .Select(metric => (metric, Value: metric.Value(m), Ceiling: Ceiling(m, metric)))
            .Where(x => x.Value > x.Ceiling)
            .Select(x => x.Ceiling == x.metric.Acceptable
                ? $"{x.metric.Name.ToLowerInvariant()} {x.Value} > {x.Ceiling}"
                : $"{x.metric.Name.ToLowerInvariant()} {x.Value} > {x.Ceiling} (its exception's ceiling)")
            .ToArray();

    /// <summary>The acceptable bound, or the exception's ceiling for a listed method.</summary>
    private static int Ceiling(MethodMetrics m, MethodMetric metric)
        => Exceptions.FirstOrDefault(e => e.Method == Key(m) && e.Metric == metric.Name)?.Max ?? metric.Acceptable;

    private static void WriteRequestedOutputs()
    {
        if (Environment.GetEnvironmentVariable(ReportVariable) is { Length: > 0 } reportPath)
            File.WriteAllText(reportPath, JsonSerializer.Serialize(Report.Value, new JsonSerializerOptions { WriteIndented = true }));

        if (Environment.GetEnvironmentVariable(SummaryVariable) is { Length: > 0 } summaryPath)
            File.AppendAllText(summaryPath, MarkdownSummary());
    }

    private static string MarkdownSummary()
    {
        var methods = ProductionMethods.ToList();
        var types = ProductionTypes.ToList();
        var report = Report.Value;
        var sb = new StringBuilder();
        sb.AppendLine("## Code health (production code)").AppendLine();
        sb.AppendLine($"{methods.Count} methods, {types.Count} types, {report.ProductionCodeLines} code lines. Targets: issue #112. " +
            $"Gate: the top of the acceptable band; {Exceptions.Length} justified exceptions.").AppendLine();
        sb.AppendLine("| Metric | Good | Acceptable | Warning | Worst | Gate |").AppendLine("|---|---|---|---|---|---|");
        foreach (var metric in MethodMetrics)
        {
            var values = methods.Select(metric.Value).ToList();
            var worst = methods.MaxBy(metric.Value)!;
            sb.AppendLine($"| {metric.Name} | {Band(values, v => v <= metric.Good)} | " +
                $"{Band(values, v => v > metric.Good && v <= metric.Acceptable)} | {Band(values, v => v > metric.Acceptable)} | " +
                $"{metric.Value(worst)} (`{worst.Type}.{worst.Name}`) | {metric.Acceptable} |");
        }
        var typeLines = types.Select(t => t.Lines).ToList();
        var biggest = types.MaxBy(t => t.Lines)!;
        sb.AppendLine($"| Type size | {Band(typeLines, v => v <= TypeGood)} | {Band(typeLines, v => v > TypeGood && v <= TypeAcceptable)} | " +
            $"{Band(typeLines, v => v > TypeAcceptable)} | {biggest.Lines} (`{biggest.Name}`) | {TypeAcceptable} |");
        sb.AppendLine($"| Duplication | | | | {report.DuplicationPercent:F2} % ({report.Duplicates.Count} blocks) | {DuplicationAcceptable} % |");

        sb.AppendLine().AppendLine("| Top cognitive | Lines | Cyclomatic | Cognitive | Location |").AppendLine("|---|---|---|---|---|");
        foreach (var m in methods.OrderByDescending(m => m.Cognitive).ThenByDescending(m => m.Cyclomatic).Take(10))
            sb.AppendLine($"| `{m.Type}.{m.Name}` | {m.Lines} | {m.Cyclomatic} | {m.Cognitive} | `{m.Location}` |");
        return sb.AppendLine().ToString();

        static string Band(List<int> values, Func<int, bool> inBand)
        {
            var count = values.Count(inBand);
            return $"{count} ({100.0 * count / values.Count:F1} %)";
        }
    }
}
