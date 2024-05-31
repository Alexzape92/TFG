package es.uca;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessor;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessorContext;

public class CustomJsonProcessor implements EsperIOKafkaInputProcessor {
    private EPRuntime runtime;

    public void init(EsperIOKafkaInputProcessorContext context) {
        this.runtime = context.getRuntime();
    }

    public void process(ConsumerRecords<Object, Object> records) {
        for (ConsumerRecord<Object, Object> record : records) {
            if (record.value() != null) {
                runtime.getEventService().sendEventJson(record.value().toString(), record.topic());
            }
        }
    }

    public void close() {
    }
}
