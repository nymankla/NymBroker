using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NymBroker.Tests.CodeHealth;

/// <summary>Per method, accessor (5+ lines) and local function. JSON names are what the code-health skill's summarize.js reads.</summary>
internal sealed record MethodMetrics(
    [property: JsonPropertyName("file")] string File,
    [property: JsonPropertyName("area")] string Area,
    [property: JsonPropertyName("line")] int Line,
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("lines")] int Lines,
    [property: JsonPropertyName("cc")] int Cyclomatic,
    [property: JsonPropertyName("cognitive")] int Cognitive,
    [property: JsonPropertyName("nesting")] int Nesting,
    [property: JsonPropertyName("parameters")] int Parameters,
    [property: JsonPropertyName("awaits")] int Awaits)
{
    public string Location => $"{File}:{Line}";
}

internal sealed record TypeMetrics(
    [property: JsonPropertyName("file")] string File,
    [property: JsonPropertyName("area")] string Area,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("kind")] string Kind,
    [property: JsonPropertyName("lines")] int Lines,
    [property: JsonPropertyName("members")] int Members,
    [property: JsonPropertyName("methods")] int Methods,
    [property: JsonPropertyName("fields")] int Fields,
    [property: JsonPropertyName("ctorParams")] int CtorParams);

internal sealed record FileMetrics(
    [property: JsonPropertyName("file")] string File,
    [property: JsonPropertyName("area")] string Area,
    [property: JsonPropertyName("lines")] int Lines,
    [property: JsonPropertyName("code")] int Code);

internal sealed record DuplicateGroup(
    [property: JsonPropertyName("files")] string[] Files,
    [property: JsonPropertyName("windows")] int Windows,
    [property: JsonPropertyName("firstLines")] string[] FirstLines);

internal sealed record CodeHealthReport(
    [property: JsonPropertyName("methods")] IReadOnlyList<MethodMetrics> Methods,
    [property: JsonPropertyName("types")] IReadOnlyList<TypeMetrics> Types,
    [property: JsonPropertyName("files")] IReadOnlyList<FileMetrics> Files,
    [property: JsonPropertyName("duplicates")] IReadOnlyList<DuplicateGroup> Duplicates,
    [property: JsonPropertyName("duplicatedLines")] int DuplicatedLines)
{
    /// <summary>Production code lines (non-blank, non-comment) in the measured files.</summary>
    [JsonPropertyName("productionCodeLines")]
    public int ProductionCodeLines => Files.Where(f => CodeMetrics.IsProduction(f.Area)).Sum(f => f.Code);

    /// <summary>Share of production code lines that are part of a block duplicated elsewhere, in percent.</summary>
    [JsonPropertyName("duplicationPercent")]
    public double DuplicationPercent => ProductionCodeLines == 0 ? 0 : 100.0 * DuplicatedLines / ProductionCodeLines;
}

/// <summary>
/// Roslyn syntax metrics for the code-health test and the code-health skill (docs/architecture.md#code-health).
/// Syntax only — no compilation — so it is fast and needs no project references.
/// </summary>
internal static class CodeMetrics
{
    /// <summary>Code lines hashed per window when looking for duplicated blocks across files.</summary>
    private const int DuplicateWindow = 10;

    /// <summary>Production = the packaged libraries and <c>shared/</c>; tests and samples are measured but not gated.</summary>
    public static bool IsProduction(string area) => area is not ("NymBroker.Tests" or "samples");

    /// <summary>The solution root: the first parent of the test assembly that holds NymBroker.slnx.</summary>
    public static string FindRepoRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "NymBroker.slnx")))
                return dir.FullName;
        throw new InvalidOperationException($"NymBroker.slnx not found above {AppContext.BaseDirectory}.");
    }

    public static CodeHealthReport Analyze(string root)
    {
        var methods = new List<MethodMetrics>();
        var types = new List<TypeMetrics>();
        var files = new List<FileMetrics>();
        var windows = new Dictionary<string, List<Window>>();

        foreach (var path in SourceFiles(root))
        {
            var rel = Path.GetRelativePath(root, path).Replace('\\', '/');
            var area = rel.Split('/')[0];
            var text = File.ReadAllText(path);
            var lines = text.Split('\n');
            files.Add(new FileMetrics(rel, area, lines.Length,
                lines.Count(l => l.Trim() is { Length: > 0 } t && !t.StartsWith("//", StringComparison.Ordinal))));

            if (IsProduction(area))
                AddWindows(windows, rel, lines);

            var syntaxRoot = CSharpSyntaxTree.ParseText(text).GetRoot();
            types.AddRange(MeasureTypes(syntaxRoot, rel, area));
            methods.AddRange(MeasureMethods(syntaxRoot, rel, area));
        }

        var duplicated = windows.Values.Where(v => v.Count > 1).SelectMany(v => v)
            .SelectMany(w => w.Lines.Select(line => (w.File, line))).Distinct().Count();
        return new CodeHealthReport(methods, types, files, GroupDuplicates(windows), duplicated);
    }

    /// <summary>A run of <see cref="DuplicateWindow"/> code lines in one file, by their line numbers.</summary>
    private sealed record Window(string File, int[] Lines);

    /// <summary>Measures the methods in one source text (used by the analyzer's own tests).</summary>
    public static IReadOnlyList<MethodMetrics> MeasureMethods(string source, string file = "Test.cs")
        => MeasureMethods(CSharpSyntaxTree.ParseText(source).GetRoot(), file, "test").ToList();

    private static IEnumerable<string> SourceFiles(string root)
        => Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            // Matched on the path below root, so a root inside .claude/worktrees still works.
            .Where(f => Path.GetRelativePath(root, f).Split(Path.DirectorySeparatorChar)
                .SkipLast(1).All(d => d is not ("bin" or "obj" or ".claude" or ".git")))
            .Order(StringComparer.Ordinal);

    private static IEnumerable<TypeMetrics> MeasureTypes(SyntaxNode root, string file, string area)
    {
        foreach (var t in root.DescendantNodes().OfType<TypeDeclarationSyntax>())
        {
            var primaryCtor = t.ParameterList?.Parameters.Count ?? 0;
            yield return new TypeMetrics(file, area, t.Identifier.Text, t.Keyword.Text, LineCount(t),
                t.Members.Count,
                t.Members.OfType<BaseMethodDeclarationSyntax>().Count(),
                t.Members.OfType<FieldDeclarationSyntax>().Sum(f => f.Declaration.Variables.Count),
                t.Members.OfType<ConstructorDeclarationSyntax>().Select(c => c.ParameterList.Parameters.Count)
                    .DefaultIfEmpty(primaryCtor).Max());
        }
    }

    private static IEnumerable<MethodMetrics> MeasureMethods(SyntaxNode root, string file, string area)
    {
        foreach (var m in root.DescendantNodes().Where(n => n is BaseMethodDeclarationSyntax or AccessorDeclarationSyntax or LocalFunctionStatementSyntax))
        {
            if (Body(m) is not { } body) continue;
            var lines = LineCount(m);
            if (m is AccessorDeclarationSyntax && lines < 5) continue;

            // Local functions are measured on their own, so their bodies do not count towards the enclosing method.
            var nodes = body.DescendantNodesAndSelf(n => n == body || n is not LocalFunctionStatementSyntax).ToList();
            var (cognitive, nesting) = Cognitive(body);
            yield return new MethodMetrics(file, area,
                m.GetLocation().GetLineSpan().StartLinePosition.Line + 1,
                m.Ancestors().OfType<TypeDeclarationSyntax>().FirstOrDefault()?.Identifier.Text ?? "",
                Name(m), lines, 1 + nodes.Count(IsDecisionPoint), cognitive, nesting, ParameterCount(m),
                nodes.Count(n => n is AwaitExpressionSyntax));
        }
    }

    private static SyntaxNode? Body(SyntaxNode m) => m switch
    {
        BaseMethodDeclarationSyntax b => (SyntaxNode?)b.Body ?? b.ExpressionBody,
        AccessorDeclarationSyntax a => (SyntaxNode?)a.Body ?? a.ExpressionBody,
        LocalFunctionStatementSyntax l => (SyntaxNode?)l.Body ?? l.ExpressionBody,
        _ => null,
    };

    private static string Name(SyntaxNode m) => m switch
    {
        MethodDeclarationSyntax md => md.Identifier.Text,
        ConstructorDeclarationSyntax => ".ctor",
        LocalFunctionStatementSyntax lf => "local:" + lf.Identifier.Text,
        AccessorDeclarationSyntax ad => (ad.Parent?.Parent as BasePropertyDeclarationSyntax) switch
        {
            PropertyDeclarationSyntax p => p.Identifier.Text,
            EventDeclarationSyntax e => e.Identifier.Text,
            _ => "this[]",
        } + "." + ad.Keyword.Text,
        OperatorDeclarationSyntax od => "op" + od.OperatorToken.Text,
        _ => m.Kind().ToString(),
    };

    private static int ParameterCount(SyntaxNode m) => m switch
    {
        BaseMethodDeclarationSyntax b => b.ParameterList.Parameters.Count,
        LocalFunctionStatementSyntax l => l.ParameterList.Parameters.Count,
        _ => 0,
    };

    private static int LineCount(SyntaxNode node)
    {
        var span = node.GetLocation().GetLineSpan();
        return span.EndLinePosition.Line - span.StartLinePosition.Line + 1;
    }

    /// <summary>Cyclomatic complexity: one plus each branch, loop, case, catch and short-circuit operator.</summary>
    private static bool IsDecisionPoint(SyntaxNode n) => n.Kind() is SyntaxKind.IfStatement or SyntaxKind.WhileStatement
        or SyntaxKind.ForStatement or SyntaxKind.ForEachStatement or SyntaxKind.ForEachVariableStatement or SyntaxKind.DoStatement
        or SyntaxKind.CaseSwitchLabel or SyntaxKind.CasePatternSwitchLabel or SyntaxKind.SwitchExpressionArm or SyntaxKind.CatchClause
        or SyntaxKind.ConditionalExpression or SyntaxKind.LogicalAndExpression or SyntaxKind.LogicalOrExpression
        or SyntaxKind.CoalesceExpression or SyntaxKind.CoalesceAssignmentExpression or SyntaxKind.ConditionalAccessExpression;

    /// <summary>
    /// Sonar-style cognitive complexity (approximation): each branch or loop adds one plus its nesting level, <c>else</c>
    /// and each run of the same <c>&amp;&amp;</c>/<c>||</c> add one; lambdas nest without adding. Also returns the deepest
    /// control-flow nesting.
    /// </summary>
    private static (int Score, int MaxNesting) Cognitive(SyntaxNode body)
    {
        int score = 0, maxNesting = 0;
        Walk(body, 0);
        return (score, maxNesting);

        void Walk(SyntaxNode node, int nesting)
        {
            foreach (var child in node.ChildNodes())
            {
                if (child is LocalFunctionStatementSyntax) continue;

                if (child is IfStatementSyntax { Parent: ElseClauseSyntax })
                {
                    // `else if` costs one, at the nesting of the `if` it continues.
                    score++;
                    Walk(child, nesting);
                    continue;
                }

                var (increment, nests, isLambda) = child switch
                {
                    IfStatementSyntax => (1 + nesting, true, false),
                    ElseClauseSyntax { Statement: not IfStatementSyntax } => (1, false, false),
                    WhileStatementSyntax or ForStatementSyntax or ForEachStatementSyntax or ForEachVariableStatementSyntax
                        or DoStatementSyntax or SwitchStatementSyntax or SwitchExpressionSyntax or CatchClauseSyntax
                        or ConditionalExpressionSyntax => (1 + nesting, true, false),
                    LambdaExpressionSyntax or AnonymousMethodExpressionSyntax => (0, true, true),
                    BinaryExpressionSyntax b when b.Kind() is SyntaxKind.LogicalAndExpression or SyntaxKind.LogicalOrExpression
                        => (b.Parent is BinaryExpressionSyntax p && p.Kind() == b.Kind() ? 0 : 1, false, false),
                    GotoStatementSyntax => (1, false, false),
                    _ => (0, false, false),
                };

                score += increment;
                var next = nests ? nesting + 1 : nesting;
                if (nests && !isLambda) maxNesting = Math.Max(maxNesting, next);
                Walk(child, next);
            }
        }
    }

    private static void AddWindows(Dictionary<string, List<Window>> windows, string file, string[] lines)
    {
        var code = lines.Select((l, i) => (Text: l.Trim(), Line: i + 1))
            .Where(x => x.Text.Length > 2 && !x.Text.StartsWith("//", StringComparison.Ordinal)
                        && !x.Text.StartsWith("using ", StringComparison.Ordinal)
                        && x.Text is not ("{" or "}" or "};" or "});" or ")" or ");"))
            .ToList();
        for (var i = 0; i + DuplicateWindow <= code.Count; i++)
        {
            var window = code.Skip(i).Take(DuplicateWindow).ToList();
            var block = string.Join("\n", window.Select(x => x.Text));
            var key = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(block)));
            if (!windows.TryGetValue(key, out var list)) windows[key] = list = [];
            list.Add(new Window(file, window.Select(x => x.Line).ToArray()));
        }
    }

    /// <summary>Windows seen more than once, grouped by the set of files they occur in.</summary>
    private static List<DuplicateGroup> GroupDuplicates(Dictionary<string, List<Window>> windows)
        => windows.Values.Where(v => v.Count > 1)
            .GroupBy(v => string.Join("|", v.Select(x => x.File).Order(StringComparer.Ordinal)))
            .Select(g => new DuplicateGroup(g.Key.Split('|').Distinct().ToArray(), g.Count(),
                g.First().Select(x => $"{x.File}:{x.Lines[0]}").ToArray()))
            .OrderByDescending(g => g.Windows)
            .ToList();
}
