package com.bigdata.mapper;


import com.bigdata.kv.key.ComDimension;
import com.bigdata.kv.key.ContactDimension;
import com.bigdata.kv.key.DateDimension;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//
public class CountDurationMapper extends TableMapper<ComDimension, Text>{
    private ComDimension comDimension = new ComDimension();
    private Text durationText = new Text();
    private Map<String, String> phoneNameMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        phoneNameMap = new HashMap<>();
        phoneNameMap.put("13171182362", "霍风浩");
        phoneNameMap.put("17418906165", "贾鑫瑜");
        phoneNameMap.put("13194555546", "余建堂");
        phoneNameMap.put("13275211900", "陈猛");
        phoneNameMap.put("18939575060", "王倩");
        phoneNameMap.put("17026053728", "杨占昊");
        phoneNameMap.put("16741935699", "刘洋");
        phoneNameMap.put("13415701165", "李伟");
        phoneNameMap.put("14081946321", "张苗");
        phoneNameMap.put("16303009156", "赵晓露");
        phoneNameMap.put("14018148812", "杨青林");
        phoneNameMap.put("15590483587", "孙凯迪");
        phoneNameMap.put("15458558266", "陈凯");
        phoneNameMap.put("19392963501", "常天罡");
        phoneNameMap.put("16534348434", "冀缨菲");
        phoneNameMap.put("18428637462", "孙良明");
        phoneNameMap.put("13690470615", "贾明灿");
        phoneNameMap.put("15539613975", "陈鑫");
        phoneNameMap.put("17816115082", "张文豪");
        phoneNameMap.put("14529464431", "刘优");
        phoneNameMap.put("19153650733", "郭振君");
        phoneNameMap.put("16479429004", "段雪鹏");
        phoneNameMap.put("15340613814", "刘海涛");
        phoneNameMap.put("19119950794", "董润华");
        phoneNameMap.put("18122332823", "高永斌");
        phoneNameMap.put("17882324598", "张文举");
        phoneNameMap.put("13094566759", "闵强");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //05_19902496992_20170312154840_15542823911_1_1288
        String rowKey = Bytes.toString(key.get());
        String[] splits = rowKey.split("_");

        // 此处从hbase表中获取列信息
        Cell[] rawCells = value.rawCells();
        String callee = null;
        String flag =null;
        String duration =null;

        for (Cell cell : rawCells){

           //String cfName =  Bytes.toString(CellUtil.cloneFamily(cell));
            String columnName =  Bytes.toString(CellUtil.cloneQualifier(cell));
            String columnVal =  Bytes.toString(CellUtil.cloneValue(cell));
            if(columnName.equals("callee")){
                callee = columnVal;
            }
            if(columnName.equals("flag")){
                flag = columnVal;
            }
            if(columnName.equals("dur")){
                duration = columnVal;
            }

        }


        // 把被叫信息扔掉，否则数据会翻一倍
        if(flag.equals("1")) return;

        //以下数据全部是主叫数据，但是也包含了被叫电话的数据
        String caller = splits[1];
        String buildTime = splits[2];
        durationText.set(duration);

        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(4, 6);
        String day = buildTime.substring(6, 8);

        //组装ComDimension
        //组装DateDimension
        ////05_19902496992_20170312154840_15542823911_1_1288
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");
        DateDimension monthDimension = new DateDimension(year, month, "-1");
        DateDimension dayDimension = new DateDimension(year, month, day);

        //组装ContactDimension
        ContactDimension callerContactDimension = new ContactDimension(caller, phoneNameMap.get(caller));

        //开始聚合主叫数据
        comDimension.setContactDimension(callerContactDimension);
        //年
        comDimension.setDateDimension(yearDimension);
        context.write(comDimension, durationText);
        //月
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, durationText);
        //日
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, durationText);

        //开始聚合被叫数据
        String name = phoneNameMap.get(callee);
        System.out.println("1-------:"+name);
        ContactDimension calleeContactDimension = new ContactDimension(callee, name);
        comDimension.setContactDimension(calleeContactDimension);
        //年
        comDimension.setDateDimension(yearDimension);
        context.write(comDimension, durationText);
        //月
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, durationText);
        //日
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, durationText);
    }
}