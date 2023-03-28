package com.geoserver.publish.task;

/**
 * @author: jingteng
 * @date: 2023/3/28 21:18
 */

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class OneTimeTaskApplication {

    @Autowired
    private MyTask myTask;

    @Autowired
    private ApplicationContext applicationContext;

    public static void main(String[] args) {
        SpringApplication.run(OneTimeTaskApplication.class, args);
    }

    @PostConstruct
    public void executeTaskAndShutdown() {
        myTask.execute();
        SpringApplication.exit(applicationContext, () -> 0);
    }
}
