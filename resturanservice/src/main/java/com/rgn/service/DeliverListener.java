package com.rgn.service;


import model.Order;
import model.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.streams.HeaderEnricher;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class DeliverListener {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;


    @KafkaListener(topics = Topics.SUCCESS_PAYMENT_ORDERS, groupId = "order" ,containerFactory = "kafkaListenerContainerFactory" )
    public void listenToFailedPayments( Order order) {
        System.out.println("send notification to kitchen and after food is ready for order  : " + order +" put it in deliverable orders");


    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        JsonDeserializer<HeaderEnricher.Container> deserializer = new JsonDeserializer<>(HeaderEnricher.Container.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        props.put(JsonDeserializer.TRUSTED_PACKAGES, Order.class.getPackage().getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Order.class);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");

        return props;
    }

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Order>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}










