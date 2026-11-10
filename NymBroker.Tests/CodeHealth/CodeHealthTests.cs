using System.Text;
using System.Text.Json;

namespace NymBroker.Tests.CodeHealth;

/// <summary>
/// Complexity gate for the production code (docs/architecture.md#code-health, issue #112). Each metric has the target
/// bands good / acceptable / warning; the gate fails above the <c>Limit</c>. Limits are a ratchet: lower one when the
/// worst value allows, towards the top of the acceptable band; never raise a limit to let new code in — split the
/// method or type instead.
/// </summary>
public sealed class CodeHealthTests
{
    /// <summary>A method metric with its target bands (inclusive upper bounds) and the gate limit.</summary>
    internal sealed record MethodMetric(string Name, Func<MethodMetrics, int> Value, int Good, int Acceptable, int Limit);

    internal static readonly MethodMetric[] MethodMetrics =
    [
        new("Lines", m => m.Lines, Good: 15, Acceptable: 30, Limit: 50),
        new("Cyclomatic", m => m.Cyclomatic, Good: 5, Acceptable: 10, Limit: 10),
        new("Cognitive", m => m.Cognitive, Good: 5, Acceptable: 15, Limit: 15),
        // The method body is level 0, so an `if` in a loop is 2: the target "at most 2 levels".
        new("Nesting", m => m.Nesting, Good: 1, Acceptable: 2, Limit: 3),
        new("Parameters", m => m.Parameters, Good: 3, Acceptable: 4, Limit: 5),
    ];

    /// <summary>Lines of one type declaration (each file of a partial class counts on its own).</summary>
    internal const int TypeGood = 199, TypeAcceptable = 500, MaxTypeLines = 500;

    /// <summary>Production code lines in a block duplicated elsewhere, in percent.</summary>
    internal const double DuplicationAcceptable = 3.0, MaxDuplicationPercent = 3.0;

    /// <summary>
    /// Public API whose parameter list can't change without a breaking release: measured, but not gated on parameters.
    /// Constructors are not gated on parameters either: the DI container fills them.
    /// </summary>
    internal static readonly HashSet<string> ParameterExceptions = new(StringComparer.Ordinal)
    {
        "EndpointHandler.InvokeAsync",          // called by every custom endpoint
        "HealthCheckResult.FromProbeAsync",     // called by every endpoint health check
        "NymBrokerImpl.PostBatchAsync",         // INymBroker.PostBatchAsync, optional parameters
    };

    /// <summary>Set to a file path to write the full JSON report (the code-health skill's summarize.js reads it).</summary>
    private const string ReportVariable = "NYMBROKER_CODE_HEALTH_REPORT";

    /// <summary>Set to a file path to append a Markdown summary (CI passes $GITHUB_STEP_SUMMARY).</summary>
    private const string SummaryVariable = "NYMBROKER_CODE_HEALTH_SUMMARY";

    private static readonly Lazy<CodeHealthReport> Report = new(() => CodeMetrics.Analyze(CodeMetrics.FindRepoRoot()));

    private static IEnumerable<MethodMetrics> ProductionMethods => Report.Value.Methods.Where(m => CodeMetrics.IsProduction(m.Area));

    private static IEnumerable<TypeMetrics> ProductionTypes => Report.Value.Types.Where(t => CodeMetrics.IsProduction(t.Area));

    [Fact]
    public void ProductionMethods_StayWithinComplexityLimits()
    {
        WriteRequestedOutputs();

        var violations = ProductionMethods
            .Select(m => (Method: m, Broken: BrokenLimits(m)))
            .Where(x => x.Broken.Length > 0)
            .OrderBy(x => x.Method.Location, StringComparer.Ordinal)
            .Select(x => $"  {x.Method.Location} {x.Method.Type}.{x.Method.Name}: {string.Join(", ", x.Broken)}")
            .ToList();

        Assert.True(violations.Count == 0,
            $"{violations.Count} production method(s) exceed the code-health limits " +
            $"({string.Join(", ", MethodMetrics.Select(x => $"{x.Name.ToLowerInvariant()} {x.Limit}"))}). " +
            "Split them up rather than raising a limit — see docs/architecture.md#code-health.\n" +
            string.Join("\n", violations));
    }

    [Fact]
    public void ProductionTypes_StayWithinSizeLimit()
    {
        var violations = ProductionTypes.Where(t => t.Lines > MaxTypeLines)
            .Select(t => $"  {t.File} {t.Kind} {t.Name}: {t.Lines} lines")
            .ToList();

        Assert.True(violations.Count == 0,
            $"{violations.Count} production type declaration(s) exceed {MaxTypeLines} lines. Split the responsibilities " +
            "into smaller types (or a partial class into files per concern) — see docs/architecture.md#code-health.\n" +
            string.Join("\n", violations));
    }

    [Fact]
    public void ProductionCode_DuplicationStaysWithinLimit()
    {
        var report = Report.Value;
        Assert.True(report.DuplicationPercent <= MaxDuplicationPercent,
            $"{report.DuplicationPercent:F2} % of the production code is duplicated (limit {MaxDuplicationPercent} %). " +
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

    private static bool IsParameterException(MethodMetrics m)
        => m.Name == ".ctor" || ParameterExceptions.Contains($"{m.Type}.{m.Name}");

    private static string[] BrokenLimits(MethodMetrics m)
        => MethodMetrics
            .Where(metric => metric.Value(m) > metric.Limit && !(metric.Name == "Parameters" && IsParameterException(m)))
            .Select(metric => $"{metric.Name.ToLowerInvariant()} {metric.Value(m)} > {metric.Limit}")
            .ToArray();

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
        sb.AppendLine($"{methods.Count} methods, {types.Count} types, {report.ProductionCodeLines} code lines. Targets: issue #112.").AppendLine();
        sb.AppendLine("| Metric | Good | Acceptable | Warning | Worst | Limit |").AppendLine("|---|---|---|---|---|---|");
        foreach (var metric in MethodMetrics)
        {
            var values = methods.Select(metric.Value).ToList();
            var worst = methods.MaxBy(metric.Value)!;
            sb.AppendLine($"| {metric.Name} | {Band(values, v => v <= metric.Good)} | " +
                $"{Band(values, v => v > metric.Good && v <= metric.Acceptable)} | {Band(values, v => v > metric.Acceptable)} | " +
                $"{metric.Value(worst)} (`{worst.Type}.{worst.Name}`) | {metric.Limit} |");
        }
        var typeLines = types.Select(t => t.Lines).ToList();
        var biggest = types.MaxBy(t => t.Lines)!;
        sb.AppendLine($"| Type size | {Band(typeLines, v => v <= TypeGood)} | {Band(typeLines, v => v > TypeGood && v <= TypeAcceptable)} | " +
            $"{Band(typeLines, v => v > TypeAcceptable)} | {biggest.Lines} (`{biggest.Name}`) | {MaxTypeLines} |");
        sb.AppendLine($"| Duplication | | | | {report.DuplicationPercent:F2} % ({report.Duplicates.Count} blocks) | {MaxDuplicationPercent} % |");

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
