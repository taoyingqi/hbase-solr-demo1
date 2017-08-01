package com.youzidata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.youzidata.util.TimeUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SolrCloudTest {
    public static final Log LOG = LogFactory.getLog(SolrCloudTest.class);
    //加载配置文件属性
    private static Config config = ConfigFactory.load("userdev_pi_solr.properties");
    private static SolrClient solrClient;

    private static Connection connection;
    private static Table table;
    private static Get get;
    private static String hbaseTable;
    List<Get> list = new ArrayList<Get>();

    static {
        final List<String> zkHosts = new ArrayList<String>();
        String zk_host = config.getString("zk_host");
        String[] data = zk_host.split(",");
        for (String zkHost : data) {
            zkHosts.add(zkHost);
        }
        String solr_baseURL = config.getString("solr_baseURL");
        solrClient = new HttpSolrClient(solr_baseURL);

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

    private void addIndex() throws Exception {
        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        String[] solr_columns = config.getString("solr_column").split(",");

        SolrInputDocument doc = new SolrInputDocument();
        String key = "";
        key = String.valueOf("2");
        doc.addField("id", key);
        // 获取需要索引的列的值并将其添加到SolrDoc
        for (int i = 0; i < solr_columns.length; i++) {
            String solrColName = solr_columns[i];
            if (solrColName.endsWith("_dt")) {
                doc.addField(solrColName, TimeUtil.convertFormat("20170615122304", TimeUtil.DATE_TIME_TYPE, TimeUtil.UTC_TYPE));
            } else {
                doc.addField(solr_columns[i], key + "-" + solr_columns[i]);
            }
        }
        docs.add(doc);
        LOG.info("docs info:" + docs + "\n");
        solrClient.add(docs);
        solrClient.commit();
    }

    public void search(String Str) throws Exception {
        SolrQuery query = new SolrQuery();
        query.setRows(100);
        query.setQuery(Str);
        LOG.info("query string: " + Str);
        QueryResponse response = solrClient.query(query);
        SolrDocumentList docs = response.getResults();
        System.out.println("文档个数：" + docs.getNumFound()); //数据总条数也可轻易获取
        System.out.println("查询时间：" + response.getQTime());
        System.out.println("查询总时间：" + response.getElapsedTime());
        for (SolrDocument doc : docs) {
            String rowkey = (String) doc.getFieldValue("id");
            get = new Get(Bytes.toBytes(rowkey));
            list.add(get);
        }

        Result[] res = table.get(list);

        for (Result rs : res) {
            Cell[] cells = rs.rawCells();

            for (Cell cell : cells) {
                System.out.println("============");
                System.out.println(new String(CellUtil.cloneRow(cell)));
                System.out.println(new String(CellUtil.cloneFamily(cell)));
                System.out.println(new String(CellUtil.cloneQualifier(cell)));
                System.out.println(new String(CellUtil.cloneValue(cell)));
                System.out.println("============");
                break;
            }
        }
        table.close();
    }

    public static void main(String[] args) throws Exception {
//        cloudSolrClient.connect();
        SolrCloudTest solrt = new SolrCloudTest();
        solrt.addIndex();
//        solrt.search("id:0005");
//        cloudSolrClient.close();
    }
}