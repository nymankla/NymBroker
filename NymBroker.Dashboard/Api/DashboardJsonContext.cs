using System.Text.Json;
using System.Text.Json.Serialization;

namespace NymBroker.Dashboard.Api;

/// <summary>Source-generated JSON for every dashboard response, so the package needs no reflection and works trimmed and AOT.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, UseStringEnumConverter = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull, NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals)]
[JsonSerializable(typeof(DashboardOverview))]
[JsonSerializable(typeof(LiveSnapshot))]
[JsonSerializable(typeof(HealthView))]
[JsonSerializable(typeof(MessagePageView))]
[JsonSerializable(typeof(ProblemView))]
internal sealed partial class DashboardJsonContext : JsonSerializerContext;
