package com.youzidata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by MT-T450 on 2017/8/1.
 */
public class HbaseTools {
    //加载配置文件属性
    static Config config = ConfigFactory.load("userdev_pi_solr.properties");
    //log记录
    private static final Logger logger = LoggerFactory.getLogger(SolrIndexTools.class);

    private static String hbaseTable;

    private static Connection connection;
    public static Table table;
    public static Get get;

    static {
        // hbase config
        Configuration configuration = HBaseConfiguration.create();
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.master", "10.0.110.121:16000");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "ambari02,ambari03,ambari01");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.set("hbase.client.pause", "30");
        configuration.set("hbase.client.retries.number", "3");
        configuration.set("hbase.rpc.timeout", "2000");
        configuration.set("hbase.client.operation.timeout", "3000");
        hbaseTable = config.getString("hbase_table");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(hbaseTable));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
