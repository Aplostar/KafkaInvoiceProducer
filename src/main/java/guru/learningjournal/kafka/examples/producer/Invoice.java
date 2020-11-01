package guru.learningjournal.kafka.examples.producer;

import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Invoice implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private String topicName;

    private KafkaProducer<Integer, PosInvoice>producer;
    Invoice(KafkaProducer<Integer,PosInvoice>producer,String topicName){
        this.producer = producer;
        this.topicName = topicName;
    }
    @Override
    public void run() {
        logger.info("Creating new Invoice");
        PosInvoice p = InvoiceGenerator.getInstance().getNextInvoice();
        logger.info("Sending new Invoice");
        producer.send(new ProducerRecord<>(topicName,p));
    }
}
