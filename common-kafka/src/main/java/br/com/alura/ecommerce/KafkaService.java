package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {


    private final KafkaConsumer<String, T> kConsumer;
    private final ConsumerFunction parse;

    KafkaService(String topic, ConsumerFunction parse, String groupId, Class<T> type,
		 Map<String, String> props) {
	this.parse = parse;
	this.kConsumer = new KafkaConsumer<>(properties(type, groupId, props));
	kConsumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(Pattern topic, ConsumerFunction parse, String groupId, Class<T> type,
		 Map<String, String> props) {
	this.parse = parse;
	this.kConsumer = new KafkaConsumer<>(properties(type, groupId, props));
	kConsumer.subscribe(topic);
    }

    void run() {
	while (true) {
	    var consRecords = kConsumer.poll(Duration.ofMillis(100));

	    if (!consRecords.isEmpty()) {
		System.out.println("Encontrado " + consRecords.count() + " registros");
		for (var result : consRecords) {
		    parse.consume(result);
		}
	    }
	}
    }

    private Properties properties(Class<T> type, String groupId, Map<String, String> props) {
	var properties = new Properties();
	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); // ID PARA PARTIÇÕES
	properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
	properties.putAll(props);
	// properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // NÚMERO DE REGISTROS QUE SERÃO
	// CONSUMIDOS
	return properties;
    }

    @Override
    public void close() {
	kConsumer.close();
    }
}
