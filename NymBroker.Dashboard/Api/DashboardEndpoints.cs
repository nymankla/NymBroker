using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Hosting;
using NymBroker.Core;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Dashboard.Metrics;

namespace NymBroker.Dashboard.Api;

/// <summary>The page and its JSON API, mapped under the dashboard's route group. Every route is read-only.</summary>
internal static class DashboardEndpoints
{
    internal const int DefaultPageSize = 50;
    internal const int MaxPageSize = 200;

    private static readonly string Version =
        typeof(INymBroker).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "";

    public static void Map(RouteGroupBuilder group)
    {
        group.MapGet("/", Index);
        group.MapGet("/assets/{name}", (HttpContext http, string name) => UiAssets.Serve(http, name));
        group.MapGet("/api/overview", OverviewAsync);
        group.MapGet("/api/live", Live);
        group.MapGet("/api/health", HealthAsync);
        group.MapGet("/api/endpoints/{name}/messages", MessagesAsync);
    }

    /// <summary>The page uses relative URLs, so it must be served from a path that ends with a slash.</summary>
    private static IResult Index(HttpContext http)
    {
        var path = http.Request.PathBase + http.Request.Path;
        return path.Value?.EndsWith('/') == true ? UiAssets.Serve(http, "index.html") : Results.Redirect(path + "/" + http.Request.QueryString);
    }

    /// <summary>What the handlers take from the request and the container, bound as one parameter.</summary>
    internal sealed record DashboardContext(HttpContext Http, INymBroker Broker, SnapshotService Snapshots, MetricCollector Metrics,
        NymBrokerDashboardOptions Options, IHostEnvironment Environment)
    {
        public bool ShowPayloads => Options.ShowPayloads ?? Environment.IsDevelopment();
    }

    /// <summary>
    /// The query string of the messages route, read by hand: the request delegate generator cannot bind nullable
    /// reference properties through <c>[AsParameters]</c>, and reading it here gives a clear message for a bad value.
    /// </summary>
    internal sealed record MessageQuery(QueueInspectionState State, int Limit, string? Cursor, DateTimeOffset? FailedSince)
    {
        public static bool TryRead(IQueryCollection query, out MessageQuery parsed, out string error)
        {
            parsed = new MessageQuery(QueueInspectionState.Pending, DefaultPageSize, null, null);
            error = "";
            if (!TryParseState(query["state"], out var state)) error = "state must be Pending, InProgress, Deferred or Failed.";
            else if (!TryParseOptional(query["limit"], int.TryParse, out int? limit)) error = "limit must be a number.";
            else if (!TryParseOptional(query["failedSince"], DateTimeOffset.TryParse, out DateTimeOffset? failedSince)) error = "failedSince must be a date and time.";
            else parsed = new MessageQuery(state, Math.Clamp(limit ?? DefaultPageSize, 1, MaxPageSize), NullIfEmpty(query["cursor"]), failedSince);
            return error.Length == 0;
        }

        private delegate bool Parser<T>(string? text, out T value);

        private static bool TryParseOptional<T>(string? text, Parser<T> parse, out T? value) where T : struct
        {
            value = null;
            if (string.IsNullOrEmpty(text)) return true;
            if (!parse(text, out var parsed)) return false;
            value = parsed;
            return true;
        }

        private static string? NullIfEmpty(string? text) => string.IsNullOrEmpty(text) ? null : text;
    }

    private static async Task<IResult> OverviewAsync([AsParameters] DashboardContext context)
    {
        var (http, options) = (context.Http, context.Options);
        NoStore(http);
        var live = await context.Snapshots.GetCurrentAsync(http.RequestAborted);
        var overview = new DashboardOverview(options.Title, Version, (int)options.RefreshInterval.TotalMilliseconds, (int)options.History.TotalSeconds,
            context.ShowPayloads, context.Broker.Describe(), context.Metrics.History(), live);
        return TypedResults.Json(overview, DashboardJsonContext.Default.DashboardOverview);
    }

    private static IResult Live(HttpContext http, SnapshotService snapshots)
    {
        NoStore(http);
        return TypedResults.ServerSentEvents(StreamAsync(snapshots, http.RequestAborted), eventType: "snapshot");
    }

    private static async IAsyncEnumerable<string> StreamAsync(SnapshotService snapshots, [EnumeratorCancellation] CancellationToken ct)
    {
        using var subscription = snapshots.Subscribe();
        await foreach (var snapshot in subscription.Reader.ReadAllAsync(ct))
            yield return JsonSerializer.Serialize(snapshot, DashboardJsonContext.Default.LiveSnapshot);
    }

    private static async Task<IResult> HealthAsync(HttpContext http, SnapshotService snapshots)
    {
        NoStore(http);
        return TypedResults.Json(await snapshots.CheckHealthAsync(http.RequestAborted), DashboardJsonContext.Default.HealthView);
    }

    private static async Task<IResult> MessagesAsync(string name, [AsParameters] DashboardContext context)
    {
        NoStore(context.Http);
        if (!MessageQuery.TryRead(context.Http.Request.Query, out var query, out var error)) return Problem(StatusCodes.Status400BadRequest, error);
        if (!TryFindInspector(context.Broker, name, out var inspector)) return Problem(StatusCodes.Status404NotFound, $"No endpoint named '{name}'.");
        if (inspector is null) return Problem(StatusCodes.Status409Conflict, $"Endpoint '{name}' cannot be inspected.");

        QueuePage page;
        try
        {
            page = await inspector.GetMessagesAsync(query.State, query.Limit, query.Cursor, query.FailedSince, context.Http.RequestAborted);
        }
        catch (ArgumentException ex)
        {
            return Problem(StatusCodes.Status400BadRequest, ex.Message);
        }

        var show = context.ShowPayloads;
        var messages = page.Messages.Select(m => MessageViews.From(m, name, show, context.Options)).ToList();
        return TypedResults.Json(new MessagePageView(messages, page.NextCursor, show), DashboardJsonContext.Default.MessagePageView);
    }

    /// <summary>A listable state (not Completed, which is only counted); Pending when none is given.</summary>
    private static bool TryParseState(string? text, out QueueInspectionState state)
        => Enum.TryParse(text ?? nameof(QueueInspectionState.Pending), ignoreCase: true, out state)
           && Enum.IsDefined(state) && state != QueueInspectionState.Completed;

    /// <summary>False when there is no such endpoint; otherwise its inspector, null when it has none.</summary>
    private static bool TryFindInspector(INymBroker broker, string name, out IQueueInspector? inspector)
    {
        try
        {
            inspector = broker.GetQueueInspector(name);
            return true;
        }
        catch (InvalidOperationException)
        {
            inspector = null;   // GetQueueInspector's answer for an unknown endpoint; the caller turns it into a 404.
            return false;
        }
    }

    private static IResult Problem(int status, string error)
        => TypedResults.Json(new ProblemView(error), DashboardJsonContext.Default.ProblemView, statusCode: status);

    private static void NoStore(HttpContext http) => http.Response.Headers.CacheControl = "no-store";
}
