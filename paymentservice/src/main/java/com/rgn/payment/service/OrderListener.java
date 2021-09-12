package com.rgn.payment.service;


import model.Order;
import model.PaymentState;
import model.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.streams.HeaderEnricher;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class OrderListener {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;


    @KafkaListener(topics = Topics.PENDING_ORDERS, groupId = "order" ,containerFactory = "kafkaListenerContainerFactory" )
    public void listenToFailedPayments(Order order) {
        if(order.getAmount()>1000D) {
            order.setPaymentStat(PaymentState.SUCCESS);
            kafkaTemplate.send(Topics.SUCCESS_PAYMENT_ORDERS, order);

        }
        else{
            order.setPaymentStat(PaymentState.FAIL);
            kafkaTemplate.send(Topics.FAILED_PAYMENT, order);
        }


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










