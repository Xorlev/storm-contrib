package storm.kafka;

import java.util.HashMap;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaPartitionConnections {
    Map<Integer, SimpleConsumer> _kafka = new HashMap<Integer, SimpleConsumer>();
    
    KafkaConfig _config;
    
    public KafkaPartitionConnections(KafkaConfig conf) {
        _config = conf;
    }
    
    public SimpleConsumer getConsumer(int partition) {
        if(!_kafka.containsKey(partition)) {
            int hostIndex = partition % _config.hosts.size();
            _kafka.put(partition, new SimpleConsumer(_config.hosts.get(hostIndex), _config.port, _config.socketTimeoutMs, _config.bufferSizeBytes));
            
        }
        return _kafka.get(partition);
    }
    
    public void close() {
        for(SimpleConsumer consumer: _kafka.values()) {
            consumer.close();
        }
    }    
}
