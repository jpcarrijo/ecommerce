package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProd<T> implements Closeable {  // Closeable - interface que encerra o processo


    private final KafkaProducer<String, T> kProducer;

    public KafkaProd() {
	this.kProducer = new KafkaProducer<>(properties());
    }

    public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
	var result = new ProducerRecord<>(topic, key, value);
	Callback callback = (data, exception) -> { // SEND É SÍNCRONO, ENTÃO É NECESSÁRIO UMA CALLBACK PARA
	    // APRESENTAR INFORMAÇÕES DE SUCESSO OU ERRO
	    if (exception != null) exception.printStackTrace();
	    System.out.println("sucesso enviando: " + data.topic() + ":::partition " + data.partition() + "/ " +
				   "offset " + data.offset() + "/ timestamp " + data.timestamp());
	};
	kProducer.send(result, callback).get();
    }

    private static Properties properties() {
	var properties = new Properties();
	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
	return properties;
    }

    @Override
    public void close() {
	kProducer.close();
    }
}
