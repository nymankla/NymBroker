using System.Data.Common;

namespace NymBroker.Shared;

internal static class PooledConnection
{
    /// <summary>Opens a new connection from the driver's pool; disposes it if opening fails.</summary>
    internal static async Task<TConnection> OpenAsync<TConnection>(TConnection connection, CancellationToken ct) where TConnection : DbConnection
    {
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
