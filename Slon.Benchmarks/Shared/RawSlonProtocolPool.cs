using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.ObjectPool;
using Npgsql;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;
using Slon.Text;
using Slon.Transport;

namespace Slon.Fortunes;

internal sealed class RawSlonProtocolPool : IAsyncDisposable
{
    const string Query = "SELECT id, message FROM fortune";
    readonly Slot[] _slots;
    readonly SlonConsumptionMode _consumptionMode;
    int _nextSlot = -1;

    RawSlonProtocolPool(Slot[] slots, SlonConsumptionMode consumptionMode)
        => (_slots, _consumptionMode) = (slots, consumptionMode);

    internal static async ValueTask<RawSlonProtocolPool> CreateAsync(
        string connectionString,
        int connectionCount,
        SlonConsumptionMode consumptionMode)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var clientOptions = new PgClientOptions
        {
            EndPoint = new DnsEndPoint(
                RequiredPostgreSqlValue("Host", builder.Host),
                builder.Port),
            Database = RequiredPostgreSqlValue("Database", builder.Database),
            Username = RequiredPostgreSqlValue("Username", builder.Username),
            Password = builder.Password,
            Ssl = new PostgreSqlSslOptions { Mode = PostgreSqlSslMode.Disable },
        };
        var factory = new PgClientProtocolFactory(
            clientOptions,
            SocketStreamConnection.CreateFactory(clientOptions.EndPoint, new TransportConnectionOptions
            {
                // Match Apex's ordinary BCL read shape for this lower-layer ceiling comparison.
                UseZeroByteReads = false,
            }));
        var slots = new Slot[connectionCount];
        var created = 0;
        try
        {
            for (; created < slots.Length; created++)
            {
                var protocol = await factory.CreateAsync().ConfigureAwait(false);
                try
                {
                    var command = await PrepareAsync(protocol).ConfigureAwait(false);
                    slots[created] = new(protocol, new ReaderDrivenCommandOptions(command)
                    {
                        EnableActivationTimeout = false,
                    });
                }
                catch
                {
                    await protocol.DisposeAsync().ConfigureAwait(false);
                    throw;
                }
            }
            return new(slots, consumptionMode);
        }
        catch
        {
            for (var i = 0; i < created; i++)
                await slots[i].Protocol.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<List<T>> LoadAsync<T>(
        Func<int, string, T> create,
        CancellationToken cancellationToken)
    {
        var slot = GetSlot();
        var flow = slot.GetFlow();
        var completion = flow.WaitForCompletionAsync();
        if (!slot.Protocol.TryQueue(flow, cancellationToken: cancellationToken))
            throw new InvalidOperationException("The selected PostgreSQL protocol is unavailable.");

        var values = CollectListPool<T>.Get(create);
        try
        {
            if (_consumptionMode is SlonConsumptionMode.Collect)
            {
                await flow.CollectAsync(values, static (state, row) =>
                {
                    var list = (CollectList<T>)state!;
                    list.Add(list.Create(row.GetValue<int>(0), row.GetValue<string>(1)));
                }, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await foreach (var result in flow)
                await foreach (var row in result)
                    values.Add(create(row.GetValue<int>(0), row.GetValue<string>(1)));
            }
            _ = await completion.ConfigureAwait(false);
            slot.ReturnFlow(flow);
            return values;
        }
        catch
        {
            CollectListPool<T>.Return(values);
            throw;
        }
    }

    public void Return<T>(List<T> values)
    {
        if (values is not CollectList<T> pooled)
            throw new ArgumentException("The list was not created by this pool.", nameof(values));
        CollectListPool<T>.Return(pooled);
    }

    Slot GetSlot()
        => _slots[(int)((uint)Interlocked.Increment(ref _nextSlot) % (uint)_slots.Length)];

    public async ValueTask DisposeAsync()
    {
        List<Exception>? errors = null;
        foreach (var slot in _slots)
        {
            try
            {
                await slot.Protocol.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (errors ??= []).Add(exception);
            }
        }
        if (errors is not null)
            throw errors.Count is 1 ? errors[0] : new AggregateException(errors);
    }

    static async ValueTask<Command> PrepareAsync(PgClientProtocol protocol)
    {
        var command = Command.Create(Query, commandName: new EncodedCString("fortunes"));
        var flow = protocol.Queue(new CommandFlow(async: true, command));
        Command? prepared = null;
        await foreach (var result in flow)
        {
            var metadata = result.GetMetadata();
            prepared = Command.Create(CommandDescriptor.CreatePrepared(
                metadata.CommandName,
                metadata.ParameterTypes.Preserve(),
                metadata.RowDescription?.Preserve()));
            await foreach (var _ in result) { }
            _ = result.GetCommandComplete();
        }
        return prepared ??
            throw new InvalidOperationException("PostgreSQL preparation returned no command result.");
    }

    static string RequiredPostgreSqlValue(string name, string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"PostgreSQL {name} is required.")
            : value;

    sealed class Slot(PgClientProtocol protocol, ReaderDrivenCommandOptions options)
    {
        readonly DefaultObjectPool<ReaderDrivenCommandFlow> _flows =
            new(new FlowPolicy(options), maximumRetained: 16);

        internal PgClientProtocol Protocol { get; } = protocol;

        internal ReaderDrivenCommandFlow GetFlow() => _flows.Get();
        internal void ReturnFlow(ReaderDrivenCommandFlow flow) => _flows.Return(flow);

        sealed class FlowPolicy(ReaderDrivenCommandOptions options)
            : PooledObjectPolicy<ReaderDrivenCommandFlow>
        {
            public override ReaderDrivenCommandFlow Create() => new(options);

            public override bool Return(ReaderDrivenCommandFlow flow)
            {
                flow.Reset();
                return true;
            }
        }
    }

    sealed class CollectList<T> : List<T>
    {
        internal CollectList() : base(16) { }

        internal Func<int, string, T> Create { get; set; } = null!;
    }

    static class CollectListPool<T>
    {
        static readonly DefaultObjectPool<CollectList<T>> Pool =
            new(new Policy(), maximumRetained: 1024);

        internal static CollectList<T> Get(Func<int, string, T> create)
        {
            var list = Pool.Get();
            list.Create = create;
            return list;
        }

        internal static void Return(CollectList<T> list) => Pool.Return(list);

        sealed class Policy : PooledObjectPolicy<CollectList<T>>
        {
            public override CollectList<T> Create() => new();

            public override bool Return(CollectList<T> list)
            {
                list.Clear();
                list.Create = null!;
                return true;
            }
        }
    }
}

internal enum SlonConsumptionMode
{
    Stream,
    Collect,
}
