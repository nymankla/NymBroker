#:package Microsoft.CodeAnalysis.CSharp@4.14.0
#:property PublishAot=false
#:property JsonSerializerIsReflectionEnabledByDefault=true
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

// usage: dotnet run health.cs -- <repoRoot> <out.json>   (copy to a temp dir first, see SKILL.md)
var root = Path.GetFullPath(args[0]);
var outJson = args[1];
var files = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
    // Exclusions are matched on the path relative to root, so a root inside .claude/worktrees still works.
    .Where(f => Path.GetRelativePath(root, f).Split(Path.DirectorySeparatorChar)
        .SkipLast(1).All(d => d is not ("bin" or "obj" or ".claude")))
    .ToList();

var methods = new List<object>();
var types = new List<object>();
var fileStats = new List<object>();
var windows = new Dictionary<string, List<(string file, int line)>>();
const int Window = 10;

foreach (var file in files)
{
    var rel = Path.GetRelativePath(root, file).Replace('\\', '/');
    var area = rel.Split('/')[0];
    var text = File.ReadAllText(file);
    var tree = CSharpSyntaxTree.ParseText(text);
    var r = tree.GetRoot();
    var lines = text.Split('\n');
    int code = lines.Count(l => { var t = l.Trim(); return t.Length > 0 && !t.StartsWith("//"); });
    fileStats.Add(new { file = rel, area, lines = lines.Length, code });

    // duplicate detection on normalized code lines (skip tests/samples)
    if (area is not ("NymBroker.Tests" or "samples"))
    {
        var norm = lines.Select((l, i) => (t: l.Trim(), i: i + 1))
            .Where(x => x.t.Length > 2 && !x.t.StartsWith("//") && !x.t.StartsWith("using ") && x.t is not ("{" or "}" or "};" or "});" or ")" or ");"))
            .ToList();
        for (int i = 0; i + Window <= norm.Count; i++)
        {
            var key = Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(string.Join("\n", norm.Skip(i).Take(Window).Select(x => x.t)))));
            if (!windows.TryGetValue(key, out var list)) windows[key] = list = [];
            list.Add((rel, norm[i].i));
        }
    }

    foreach (var t in r.DescendantNodes().OfType<TypeDeclarationSyntax>())
    {
        var span = t.GetLocation().GetLineSpan();
        types.Add(new
        {
            file = rel, area, name = t.Identifier.Text, kind = t.Keyword.Text,
            lines = span.EndLinePosition.Line - span.StartLinePosition.Line + 1,
            members = t.Members.Count,
            methods = t.Members.OfType<BaseMethodDeclarationSyntax>().Count(),
            fields = t.Members.OfType<FieldDeclarationSyntax>().Sum(f => f.Declaration.Variables.Count),
            ctorParams = t.Members.OfType<ConstructorDeclarationSyntax>().Select(c => c.ParameterList.Parameters.Count).DefaultIfEmpty(t is ClassDeclarationSyntax { ParameterList: { } pl } ? pl.Parameters.Count : 0).Max(),
        });
    }

    foreach (var m in r.DescendantNodes().Where(n => n is BaseMethodDeclarationSyntax or AccessorDeclarationSyntax or LocalFunctionStatementSyntax))
    {
        SyntaxNode? body = m switch
        {
            BaseMethodDeclarationSyntax b => (SyntaxNode?)b.Body ?? b.ExpressionBody,
            AccessorDeclarationSyntax a => (SyntaxNode?)a.Body ?? a.ExpressionBody,
            LocalFunctionStatementSyntax l => (SyntaxNode?)l.Body ?? l.ExpressionBody,
            _ => null,
        };
        if (body is null) continue;
        var span = m.GetLocation().GetLineSpan();
        int len = span.EndLinePosition.Line - span.StartLinePosition.Line + 1;
        if (m is AccessorDeclarationSyntax && len < 5) continue;
        string name = m switch
        {
            MethodDeclarationSyntax md => md.Identifier.Text,
            ConstructorDeclarationSyntax cd => ".ctor",
            LocalFunctionStatementSyntax lf => "local:" + lf.Identifier.Text,
            AccessorDeclarationSyntax ad => (ad.Parent?.Parent as PropertyDeclarationSyntax)?.Identifier.Text + "." + ad.Keyword.Text,
            OperatorDeclarationSyntax od => "op" + od.OperatorToken.Text,
            _ => m.Kind().ToString(),
        };
        var owner = m.Ancestors().OfType<TypeDeclarationSyntax>().FirstOrDefault()?.Identifier.Text ?? "";
        int prms = m switch { BaseMethodDeclarationSyntax b => b.ParameterList.Parameters.Count, LocalFunctionStatementSyntax l => l.ParameterList.Parameters.Count, _ => 0 };
        // exclude nested local functions / lambdas bodies from the parent's count? keep them (they are part of the method's reading cost) except local functions which are reported separately
        var nodes = body.DescendantNodesAndSelf(n => n == body || n is not LocalFunctionStatementSyntax).ToList();
        int cc = 1 + nodes.Count(n => n.Kind() is SyntaxKind.IfStatement or SyntaxKind.WhileStatement or SyntaxKind.ForStatement
            or SyntaxKind.ForEachStatement or SyntaxKind.ForEachVariableStatement or SyntaxKind.DoStatement or SyntaxKind.CaseSwitchLabel
            or SyntaxKind.CasePatternSwitchLabel or SyntaxKind.SwitchExpressionArm or SyntaxKind.CatchClause or SyntaxKind.ConditionalExpression
            or SyntaxKind.LogicalAndExpression or SyntaxKind.LogicalOrExpression or SyntaxKind.CoalesceExpression or SyntaxKind.CoalesceAssignmentExpression
            or SyntaxKind.ConditionalAccessExpression);
        var (cog, depth) = Cognitive(body);
        int awaits = nodes.Count(n => n is AwaitExpressionSyntax);
        methods.Add(new { file = rel, area, line = span.StartLinePosition.Line + 1, type = owner, name, lines = len, cc, cognitive = cog, nesting = depth, parameters = prms, awaits });
    }
}

var dups = windows.Where(kv => kv.Value.Select(v => v.file).Distinct().Count() > 1 || kv.Value.Count > 1)
    .Select(kv => kv.Value).ToList();
// collapse consecutive windows into clone groups keyed by file-set
var dupGroups = dups.GroupBy(g => string.Join("|", g.Select(x => x.file).OrderBy(x => x)))
    .Select(g => new { files = g.Key.Split('|').Distinct().ToArray(), windows = g.Count(), firstLines = g.First().Select(x => $"{x.file}:{x.line}").ToArray() })
    .OrderByDescending(g => g.windows).ToList();

File.WriteAllText(outJson, JsonSerializer.Serialize(new { methods, types, files = fileStats, duplicates = dupGroups }, new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine($"files={files.Count} methods={methods.Count} types={types.Count} dupGroups={dupGroups.Count}");

// Sonar-style cognitive complexity (approximation)
static (int score, int maxDepth) Cognitive(SyntaxNode body)
{
    int score = 0, maxDepth = 0;
    void Walk(SyntaxNode n, int nest)
    {
        foreach (var c in n.ChildNodes())
        {
            if (c is LocalFunctionStatementSyntax) continue;
            int inc = 0; bool nests = false;
            switch (c)
            {
                case IfStatementSyntax ifs:
                    bool elseIf = ifs.Parent is ElseClauseSyntax;
                    inc = elseIf ? 1 : 1 + nest; nests = true;
                    if (elseIf) { score += inc; maxDepth = Math.Max(maxDepth, nest); Walk(c, nest); continue; }
                    break;
                case ElseClauseSyntax e when e.Statement is not IfStatementSyntax: inc = 1; break;
                case WhileStatementSyntax or ForStatementSyntax or ForEachStatementSyntax or ForEachVariableStatementSyntax or DoStatementSyntax
                    or SwitchStatementSyntax or SwitchExpressionSyntax or CatchClauseSyntax or ConditionalExpressionSyntax:
                    inc = 1 + nest; nests = true; break;
                case ParenthesizedLambdaExpressionSyntax or SimpleLambdaExpressionSyntax or AnonymousMethodExpressionSyntax:
                    nests = true; break;
                case BinaryExpressionSyntax b when b.Kind() is SyntaxKind.LogicalAndExpression or SyntaxKind.LogicalOrExpression:
                    if (b.Parent is not BinaryExpressionSyntax pb || pb.Kind() != b.Kind()) inc = 1; break;
                case GotoStatementSyntax: inc = 1; break;
            }
            score += inc;
            int next = nests ? nest + 1 : nest;
            if (nests && c is not (ParenthesizedLambdaExpressionSyntax or SimpleLambdaExpressionSyntax or AnonymousMethodExpressionSyntax)) maxDepth = Math.Max(maxDepth, next);
            Walk(c, next);
        }
    }
    Walk(body, 0);
    return (score, maxDepth);
}
