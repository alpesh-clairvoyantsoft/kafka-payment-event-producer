package com.clairvoyant.kafka;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import com.clairvoyant.model.PaymentEvent;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

@SpringBootApplication
public class PaymentEventProducer implements CommandLineRunner {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.topic}")
	private String topic;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void produceData() {

		System.out.println(bootstrapServers);
		System.out.println(topic);

		System.out.println("Enter some strings !");
		List<String> list = new ArrayList<String>();

		Gson gson = new Gson();

		int i = 0;

		boolean loop = true;
		while (loop) {
//			String json = gson.toJson(new PaymentEvent("Id_" + i, new Float(1000.0 + i), "location_" + i, new Timestamp(System.currentTimeMillis())));
    	 
			ObjectMapper mapper = new ObjectMapper();
			 
			String json="";
			try {
				json = mapper.writeValueAsString(new PaymentEvent("Id_" + i, new Float(1000.0 + i), "location_" + i, new Timestamp(System.currentTimeMillis())));
			} catch (JsonGenerationException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (JsonMappingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			kafkaTemplate.send(topic, json);
			System.out.println("Send message: " + json);
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			i++;
		}
	}

	@Override
	public void run(String... args) throws Exception {
		produceData();

	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(PaymentEventProducer.class, args);
	}

}
