package com.youzidata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Hello world!
 */
public class App {
    static Config config = ConfigFactory.load("userdev_pi_solr.properties");

    public static void main(String[] args) {
        System.out.println(config.getString("solr_collection"));
        System.out.println("Hello World!");
    }
}
