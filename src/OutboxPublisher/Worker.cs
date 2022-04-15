using System.Text.Json;
using Events;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using Polly;

namespace OutboxPublisher;

// https://www.npgsql.org/doc/replication.html

public class Worker : BackgroundService
{
    // don't put it in a const in prod 🙂
    private const string ConnectionString =
        "server=postgres;port=5432;user id=user;password=pass;database=PostgresChangeDataCaptureOutboxSample";

    /*
     * From PostgreSQL docs:
     * Replication slots provide an automated way to ensure that the master does not remove WAL segments until they
     * have been received by all standbys, and that the master does not remove rows which could cause a recovery conflict even when the standby is disconnected.
     */
    private const string SlotName = "outbox_slot";

    /*
     * From PostgreSQL docs:
     * CREATE PUBLICATION adds a new publication into the current database. The publication name must be distinct from the name of any existing publication in the current database.
     * A publication is essentially a group of tables whose data changes are intended to be replicated through logical replication. See Section 31.1 for details about how publications fit into the logical replication setup.
    */
    private const string PublicationName = "outbox_publication";

    private readonly IEventPublisher _publisher;
    private readonly ILogger<Worker> _logger;

    public Worker(IEventPublisher publisher, ILogger<Worker> logger)
    {
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await InitializeAsync(stoppingToken);

        await using var connection = new LogicalReplicationConnection(ConnectionString);
        await connection.Open(stoppingToken);

        var slot = new PgOutputReplicationSlot(SlotName);
        /*
         * From PostgreSQL docs:
         * publicationName: Comma separated list of publication names for which to subscribe (receive changes). The individual publication names are treated as standard objects names and can be quoted the same as needed.
         * protocolVersion: Protocol version. Currently versions 1 and 2 are supported. The version 2 is supported only for server version 14 and above, and it allows streaming of large in-progress transactions.
         */
        var replicationOptions = new PgOutputReplicationOptions(publicationName: PublicationName, protocolVersion: 2);

        var outboxMessage = new Dictionary<string, object>();

        await foreach (var replicationMessage in connection.StartReplication(slot, replicationOptions, stoppingToken))
        {
            if (replicationMessage is InsertMessage insertMessage)
            {
                var i = 0;
                await foreach (var column in insertMessage.NewRow)
                {
                    outboxMessage[insertMessage.Relation.Columns[i].ColumnName] = await column.Get(stoppingToken);
                    ++i;
                }

                OrderEventBase @event = outboxMessage["Type"] switch
                {
                    nameof(OrderCreated) => JsonSerializer.Deserialize<OrderCreated>((string)outboxMessage["Payload"])!,
                    nameof(OrderDelivered) => JsonSerializer.Deserialize<OrderDelivered>((string)outboxMessage["Payload"])!,
                    nameof(OrderCancelled) => JsonSerializer.Deserialize<OrderCancelled>((string)outboxMessage["Payload"])!
                };
                
                // ideally, we'd like to optimize things and publish in batches instead of one event at a time but this is good enough for a demo 🙂
                await _publisher.PublishAsync(@event);
            }
            else
            {
                _logger.LogDebug("Ignoring message of type {type}", replicationMessage.GetType().Name);
            }
            
            // So Npgsql can inform the server which WAL files can be removed/recycled
            // Like the event publishing, could probably be optimized by calling after a few events are processed, instead of one at a time.
            connection.SetReplicationStatus(replicationMessage.WalEnd);
        }
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        var policy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                50,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, timeSpan, ctx) => { _logger.LogError(ex, "Failed to initialize, retry count: {retryCount}", ctx.Count); });

        await policy.ExecuteAsync(async (ct) =>
            {
                await using var connection = new NpgsqlConnection(ConnectionString);
                await connection.OpenAsync(ct);

                if (!await PublicationExistsAsync(connection, ct))
                {
                    await CreatePublicationAsync(connection, ct);
                }

                if (!await SlotExistsAsync(connection, ct))
                {
                    await CreateSlotAsync(connection, ct);
                }
            },
            ct);

        static async Task<bool> PublicationExistsAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var checkIfPublicationExistsCommand =
                new NpgsqlCommand("SELECT EXISTS(select 1 from pg_publication WHERE pubname =  @publicationName);",
                    connection)
                {
                    Parameters = {new NpgsqlParameter("publicationName", PublicationName)}
                };

            var publicationExists = (bool) await checkIfPublicationExistsCommand.ExecuteScalarAsync(ct);
            return publicationExists;
        }

        static async Task CreatePublicationAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var createPublicationCommand =
                new NpgsqlCommand(
                    $"CREATE PUBLICATION {PublicationName} FOR TABLE \"OutboxMessages\" WITH (publish = 'insert');",
                    connection);
            await createPublicationCommand.ExecuteNonQueryAsync(ct);
        }

        static async Task<bool> SlotExistsAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            await using var checkIfSlotExistsCommand =
                new NpgsqlCommand(
                    "SELECT EXISTS(select 1 from pg_replication_slots WHERE slot_name =  @slotName);",
                    connection)
                {
                    Parameters = {new NpgsqlParameter("slotName", SlotName)}
                };

            var slotExists = (bool) await checkIfSlotExistsCommand.ExecuteScalarAsync(ct);
            return slotExists;
        }

        static async Task CreateSlotAsync(NpgsqlConnection connection, CancellationToken ct)
        {
            // pgoutput is the logical replication output plugin
            
            await using var createSlotCommand = new NpgsqlCommand(
                "SELECT * FROM pg_create_logical_replication_slot(@slotName, 'pgoutput');",
                connection)
            {
                Parameters = {new NpgsqlParameter("slotName", SlotName)}
            };
            await createSlotCommand.ExecuteNonQueryAsync(ct);
        }
    }
}