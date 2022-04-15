namespace Events;

public interface IEventPublisher
{
    Task PublishAsync(OrderEventBase @event);
}