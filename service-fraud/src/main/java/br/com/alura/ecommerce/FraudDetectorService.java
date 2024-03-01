package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {


    public static void main(String[] args) {
	var fraud = new FraudDetectorService();
	try (var service = new KafkaService<Order>("ECOMMERCE_NEW_ORDER",
					      fraud::parse,
					      FraudDetectorService.class.getSimpleName(),
					      Order.class,
					      Map.of())) {
	    service.run();
	}
    }

    private void parse(ConsumerRecord<String, Order> result) {
	System.out.println("-------------------------------------------");
	System.out.println("Processando novo pedido, checando fraude");
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
	System.out.println("Pedido processado!");
    }
}
