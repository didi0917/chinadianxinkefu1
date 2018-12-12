package com.bigdata;

import com.bigdata.utils.HbaseDriver;
import com.bigdata.utils.HbaseUtils;
import com.bigdata.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class HBaseDao {

    String tableName;
    String cf;
    String ns;

    public HBaseDao() {

        // 1、判断是否有namespace,如果没有创建，如果有跳过
        // 2、判断是否有hbase表，如果没有就创建
        tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        cf = PropertiesUtil.getProperty("hbase.calllog.columnFamily");
        ns = PropertiesUtil.getProperty("hbase.calllog.namespace");


        // ctrl + alt  + t  快速环绕一段代码
        try {
            boolean result = HbaseUtils.existsTable(HbaseDriver.getHBaseAdmin(), tableName);
            System.out.println("result:" + result);
            if (!result) {
                HbaseUtils.initNameSpace(HbaseDriver.getHBaseAdmin(), ns);
                HbaseUtils.createTable(tableName, cf);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //17418906165,19119950794,2018-08-06 15:32:32,0465
    public void put(String value) throws Exception {
        /*
           3、插入数据
         */
        if (null == value || value.equals("")) {
            System.out.println("数据为空");
            return;
        }


        String[] split = value.split(",");
        String caller = split[0];
        String callee = split[1];
        String time = split[2];
        String dur = split[3];
        // 没有rowkey
        String rowkey = getRowKey(caller, time, 6);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("caller"), Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("callee"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("time"), Bytes.toBytes(time));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dur"), Bytes.toBytes(dur));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes("0")); // 0代表主叫，1 代表被叫


        String rowkey2 = getRowKey(callee, time, 6);
        Put put2 = new Put(Bytes.toBytes(rowkey2));
        put2.addColumn(Bytes.toBytes(cf), Bytes.toBytes("caller"), Bytes.toBytes(callee));
        put2.addColumn(Bytes.toBytes(cf), Bytes.toBytes("callee"), Bytes.toBytes(caller));
        put2.addColumn(Bytes.toBytes(cf), Bytes.toBytes("time"), Bytes.toBytes(time));
        put2.addColumn(Bytes.toBytes(cf), Bytes.toBytes("dur"), Bytes.toBytes(dur));
        put2.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes("1")); // 0代表主叫，1 代表被叫"

        Table table = HbaseDriver.getConnection().getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        puts.add(put2);
        HbaseUtils.putToHBase(table, puts);

        System.out.println("数据插入成功：" + value);

    }

    // 2017-08-12   2017

    // 01_18137884406_20170831070932
    public String getRowKey(String caller, String datetime, int regions) {

        // 取手机号码后四位
        String phoneEnd = caller.substring(caller.length() - 4);
        // 获取日期的年月
        String sj = datetime.replace("-", "")
                .replace(" ", "")
                .replace(":", "");
        String ym = sj.substring(0, 6);

        Integer num = Integer.parseInt(phoneEnd) ^ Integer.parseInt(ym);
        int region = num.hashCode() % regions;
        String regin_num = new DecimalFormat("00").format(region);

        StringBuilder sb = new StringBuilder();
        sb.append(regin_num).append("_").append(caller).append("_").append(sj);

        return sb.toString();

    }
}
