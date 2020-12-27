package com.group.kaka.inventory.config;

import com.group.kaka.inventory.consumer.InventoryConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerRunner  implements ApplicationRunner {

    @Autowired
    InventoryConsumer inventoryConsumer;
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Start the Application .... ");
        inventoryConsumer.consumeInventory();
    }
}
