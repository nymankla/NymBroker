using System.Text;
using System.Text.Json;

namespace NymBroker.Tests.CodeHealth;

/// <summary>
/// Complexity gate for the production code (docs/architecture.md#code-health). The limits are a ratchet: they were set
/// to the worst production method when the gate was added. When a refactor lowers the worst value, lower the limit with
/// it; never raise a limit to let new code in — split the method instead.
/// </summary>
public sealed class CodeHealthTests
{
    internal const int MaxLines = 63;
    internal const int MaxCyclomatic = 16;
    internal const int MaxCognitive = 20;
    internal const int MaxNesting = 4;
    internal const int MaxParameters = 8;

    /// <summary>Set to a file path to write the full JSON report (the code-health skill's summarize.js reads it).</summary>
    private const string ReportVariable = "NYMBROKER_CODE_HEALTH_REPORT";

    /// <summary>Set to a file path to append a Markdown summary (CI passes $GITHUB_STEP_SUMMARY).</summary>
    private const string SummaryVariable = "NYMBROKER_CODE_HEALTH_SUMMARY";

    private static readonly Lazy<CodeHealthReport> Report = new(() => CodeMetrics.Analyze(CodeMetrics.FindRepoRoot()));

    private static IEnumerable<MethodMetrics> ProductionMethods => Report.Value.Methods.Where(m => CodeMetrics.IsProduction(m.Area));

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
            $"(lines {MaxLines}, cyclomatic {MaxCyclomatic}, cognitive {MaxCognitive}, nesting {MaxNesting}, parameters {MaxParameters}). " +
            "Split them up rather than raising a limit — see docs/architecture.md#code-health.\n" +
            string.Join("\n", violations));
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

    private static string[] BrokenLimits(MethodMetrics m)
    {
        var broken = new List<string>();
        if (m.Lines > MaxLines) broken.Add($"{m.Lines} lines > {MaxLines}");
        if (m.Cyclomatic > MaxCyclomatic) broken.Add($"cyclomatic {m.Cyclomatic} > {MaxCyclomatic}");
        if (m.Cognitive > MaxCognitive) broken.Add($"cognitive {m.Cognitive} > {MaxCognitive}");
        if (m.Nesting > MaxNesting) broken.Add($"nesting {m.Nesting} > {MaxNesting}");
        if (m.Parameters > MaxParameters) broken.Add($"{m.Parameters} parameters > {MaxParameters}");
        return [.. broken];
    }

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
        var sb = new StringBuilder();
        sb.AppendLine("## Code health (production code)").AppendLine();
        sb.AppendLine($"{methods.Count} methods, {Report.Value.Duplicates.Count} duplicated 10-line blocks across files.").AppendLine();
        sb.AppendLine("| Metric | Worst | Limit |").AppendLine("|---|---|---|");
        Row("Lines", m => m.Lines, MaxLines);
        Row("Cyclomatic", m => m.Cyclomatic, MaxCyclomatic);
        Row("Cognitive", m => m.Cognitive, MaxCognitive);
        Row("Nesting", m => m.Nesting, MaxNesting);
        Row("Parameters", m => m.Parameters, MaxParameters);
        sb.AppendLine().AppendLine("| Top cognitive | Lines | Cyclomatic | Cognitive | Location |").AppendLine("|---|---|---|---|---|");
        foreach (var m in methods.OrderByDescending(m => m.Cognitive).ThenByDescending(m => m.Cyclomatic).Take(10))
            sb.AppendLine($"| `{m.Type}.{m.Name}` | {m.Lines} | {m.Cyclomatic} | {m.Cognitive} | `{m.Location}` |");
        return sb.AppendLine().ToString();

        void Row(string name, Func<MethodMetrics, int> value, int limit)
        {
            var worst = methods.MaxBy(value)!;
            sb.AppendLine($"| {name} | {value(worst)} (`{worst.Type}.{worst.Name}`) | {limit} |");
        }
    }
}
