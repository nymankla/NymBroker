using System.Collections.Frozen;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;
using Microsoft.Net.Http.Headers;

namespace NymBroker.Dashboard.Api;

/// <summary>
/// The page, script, style and icon, embedded in the assembly: nothing is loaded from elsewhere, so the dashboard works
/// offline and behind a strict Content-Security-Policy. Each is served with an ETag, so a reload costs a 304.
/// </summary>
internal static class UiAssets
{
    internal sealed record Asset(byte[] Content, string ContentType, string ETag);

    private static readonly Lazy<FrozenDictionary<string, Asset>> All = new(Load);

    private static readonly FrozenDictionary<string, string> ContentTypes = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [".html"] = "text/html; charset=utf-8",
        [".css"] = "text/css; charset=utf-8",
        [".js"] = "text/javascript; charset=utf-8",
        [".svg"] = "image/svg+xml",
    }.ToFrozenDictionary(StringComparer.Ordinal);

    public static Asset? Find(string name) => All.Value.GetValueOrDefault(name);

    private static FrozenDictionary<string, Asset> Load()
    {
        const string prefix = "NymBroker.Dashboard.Ui.";
        var assembly = typeof(UiAssets).Assembly;
        var assets = new Dictionary<string, Asset>(StringComparer.Ordinal);
        foreach (var resource in assembly.GetManifestResourceNames().Where(r => r.StartsWith(prefix, StringComparison.Ordinal)))
        {
            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var buffer = new MemoryStream();
            stream.CopyTo(buffer);
            var content = buffer.ToArray();
            var name = resource[prefix.Length..];
            assets[name] = new Asset(content, ContentTypes.GetValueOrDefault(Path.GetExtension(name), "application/octet-stream"),
                $"\"{Convert.ToHexString(SHA256.HashData(content), 0, 8)}\"");
        }
        return assets.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public static IResult Serve(HttpContext http, string name)
    {
        if (Find(name) is not { } asset) return Results.NotFound();
        http.Response.Headers.CacheControl = "no-cache";
        http.Response.Headers.ETag = asset.ETag;
        var ifNoneMatch = http.Request.Headers[HeaderNames.IfNoneMatch].ToString();
        return ifNoneMatch == asset.ETag ? Results.StatusCode(StatusCodes.Status304NotModified) : Results.Bytes(asset.Content, asset.ContentType);
    }
}
