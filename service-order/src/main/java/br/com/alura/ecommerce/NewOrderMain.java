package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
	try (var kafkaOrder = new KafkaProd<Order>()) {
	    try (var kafkaEmail = new KafkaProd<String>()) {

		for (var i = 0; i < 10; i++) {
		    // CHAVES SÃO IMPORTANTES PARA PARALELIZAR O PROCESSAMENTO DE MSG
		    // EM UM TÓPICO DENTRO DE UM MESMO GRUPO
		    var userId = UUID.randomUUID().toString();
		    var orderId = UUID.randomUUID().toString();
		    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
		    var order = new Order(userId, orderId, amount);
		    kafkaOrder.send("ECOMMERCE_NEW_ORDER", userId, order);

		    var email = "Thank you for your order! We are processing your order";
		    kafkaEmail.send("ECOMMERCE_SEND_EMAIL", userId, email);
		}
	    }
	}
    }
}
