package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {


    public static void main(String[] args) {
	var log = new LogService();
	try (var service = new KafkaService<String>(Pattern.compile("ECOMMERCE.*"),
					      log::parse,
					      LogService.class.getSimpleName(),
					      String.class,
					      Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						     StringDeserializer.class.getName()))) {
	    service.run();
	}
    }

    private void parse(ConsumerRecord<String, String> result) {
	System.out.println("-------------------------------------------");
	System.out.println("LOG!" + result.topic());
	System.out.println(result.key());
	System.out.println(result.value());
	System.out.println(result.partition());
	System.out.println(result.offset());
	try {
	    Thread.sleep(5000);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	    Thread.currentThread().interrupt();
	}
    }
}
