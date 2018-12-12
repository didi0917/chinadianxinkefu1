package com.bigdata.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Hbase管理类,相当于工厂类
 * 此处加一个注释
 * 哈哈哈哈
 */
public class HbaseDriver {

    public static Configuration conf = null;
    private static Connection conn;
    private static int POOL_MAX_SIEZE = 500;

    public static Configuration getHbaseConf() {
        return conf;
    }

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "mini01,mini02,mini03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master.port", "60000");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static HBaseAdmin getHBaseAdmin() throws IOException {
        HBaseAdmin hbaseAdmin = null;


        try {
            hbaseAdmin = (HBaseAdmin) (conn.getAdmin());
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
        return hbaseAdmin;
    }


    public static synchronized Table getHtable(String tableName) throws IOException {
        if (conn != null) {
            return conn.getTable(TableName.valueOf(tableName));
        } else {
            try {
                conn = ConnectionFactory.createConnection(conf);
                return conn.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;//正常情况下此处运行不到
    }

    public static Connection getConnection() {
        return conn;
    }

    public static synchronized void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        HBaseAdmin hAdmin = null;
        try {
            hAdmin = getHBaseAdmin();
            TableName[] names = hAdmin.listTableNames();
            for (TableName name : names) {
                System.out.println(name.getNameAsString());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                hAdmin.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

