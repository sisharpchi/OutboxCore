namespace OutboxCore.Kafka.Configuration;

public class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "outbox-messages";
}
