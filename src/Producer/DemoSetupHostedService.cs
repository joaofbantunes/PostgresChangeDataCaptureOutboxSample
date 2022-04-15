using Producer.Data;

namespace Producer;

public class DemoSetupHostedService : IHostedService
{
    private readonly IServiceScopeFactory _scopeFactory;

    public DemoSetupHostedService(IServiceScopeFactory scopeFactory) => _scopeFactory = scopeFactory;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await InitializeDatabaseAsync();

        async Task InitializeDatabaseAsync()
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope
                .ServiceProvider
                .GetRequiredService<SampleDbContext>();

            await db
                .Database
                .EnsureCreatedAsync(cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        // no-op
        return Task.CompletedTask;
    }
}