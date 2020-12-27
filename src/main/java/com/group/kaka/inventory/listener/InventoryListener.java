package com.group.kaka.inventory.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Slf4j
@Component
public class InventoryListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        log.info("I will be executed before Partition revoked and ready for rebalancing");

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        log.info("I will be executed after Partition assigned  and ready for consuming ..");
    }
}
