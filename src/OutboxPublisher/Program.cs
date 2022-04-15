using Events;
using Events.Kafka;
using OutboxPublisher;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddSingleton<IEventPublisher, KafkaEventPublisher>();
        services.AddSingleton(typeof(JsonEventSerializer<>));
        services.AddLogging(loggingBuilder => loggingBuilder.AddSeq(serverUrl: "http://seq:5341"));
        services.AddHostedService<Worker>();
    })
    .Build();

host.Run();
