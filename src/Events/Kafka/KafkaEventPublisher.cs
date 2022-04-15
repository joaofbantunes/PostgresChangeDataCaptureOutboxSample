using System.Text;
using System.Text.Unicode;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Events.Kafka;

public class KafkaEventPublisher : IEventPublisher
{
    private readonly ILogger<KafkaEventPublisher> _logger;
    private readonly IProducer<Guid, OrderEventBase> _producer;

    public KafkaEventPublisher(JsonEventSerializer<OrderEventBase> serializer, ILogger<KafkaEventPublisher> logger)
    {
        _logger = logger;
            
        var config = new ProducerConfig
        {
            BootstrapServers = "broker:9092",
        };
            
        _producer = new ProducerBuilder<Guid, OrderEventBase>(config)
            .SetKeySerializer(GuidSerializer.Instance)
            .SetValueSerializer(serializer)
            .Build();
    }

    public async Task PublishAsync(OrderEventBase @event)
    {
        _logger.LogInformation(
            "Publishing event {eventId}, of type {eventType}!",
            @event.Id,
            @event.GetType().Name);

        var message = new Message<Guid, OrderEventBase>
        {
            // use order id as key so Kafka maintains order between events for the same entity
            Key = @event.OrderId,
            Value = @event,
            Headers = new Headers()
        };
        message.Headers.Add("eventType",  Encoding.UTF8.GetBytes(@event.Type));
        await _producer.ProduceAsync("order.events", message);
    }
}