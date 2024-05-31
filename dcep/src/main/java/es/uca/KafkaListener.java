package es.uca;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

public class KafkaListener implements UpdateListener {
    @SuppressWarnings("rawtypes")
    private KafkaProducer producer;
    String topic;

    @SuppressWarnings("rawtypes")
    public KafkaListener(KafkaProducer producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        for (EventBean event : newEvents) {
            System.out.println("Enviamos: " + event.getUnderlying().toString());
            producer.send(new ProducerRecord(topic, event.getUnderlying().toString()));
        }
    }
}
