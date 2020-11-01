package guru.learningjournal.kafka.examples.producer;

import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class InvoiceDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        // Setup Kafka Producer properties
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            properties.load(inputStream);
            properties.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // create the Kafka Producer
        KafkaProducer<Integer, PosInvoice>producer = new KafkaProducer<Integer, PosInvoice>(properties);
        Thread[]invoices = new Thread[3];
        logger.info("Starting invoice producer threads");
        for(int i = 0;i<3;i++)
        {
            invoices[i]=new Thread(new Invoice(producer,AppConfigs.topicName));
            invoices[i].start();
        }
        try{
            for(Thread t:invoices) t.join();
        }catch(InterruptedException e){
            logger.error("Main thread interrupted");
        }finally {
            producer.close();
            logger.info("Finished invoice producer demo.. ");
        }
    }

}
