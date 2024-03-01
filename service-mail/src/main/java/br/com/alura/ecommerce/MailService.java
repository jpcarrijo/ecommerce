package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class MailService {


    public static void main(String[] args) {
	var mailService = new MailService();
	try (var service = new KafkaService<String>("ECOMMERCE_SEND_EMAIL",
					      mailService::parse,
					      MailService.class.getSimpleName(),
					      String.class,
					      Map.of())) {
	    service.run();
	}
    }

    private void parse(ConsumerRecord<String, String> result) {
	System.out.println("-------------------------------------------");
	System.out.println("Send email!");
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
	System.out.println("Mail sent!");
    }
}
