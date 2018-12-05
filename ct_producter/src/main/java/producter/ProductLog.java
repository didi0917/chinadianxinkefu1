package producter;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 堂哥到此一游
 */

public class ProductLog {
    private String startTime = "2018-01-01";
    private String endTime = "2018-12-31";

    //这个是测试一下的代码

    //生产数据
    //用于存放待随机的电话号码
    private List<String> phoneList = new ArrayList<>();
    private Map<String, String> phoneNameMap = new HashMap<>();

    public void initPhone() {
        phoneList.add("13171182362");
        phoneList.add("17418906165");
        phoneList.add("13194555546");
        phoneList.add("13275211900");
        phoneList.add("18939575060");
        phoneList.add("17026053728");
        phoneList.add("16741935699");
        phoneList.add("13415701165");
        phoneList.add("14081946321");
        phoneList.add("16303009156");
        phoneList.add("14018148812");
        phoneList.add("15590483587");
        phoneList.add("15458558266");
        phoneList.add("19392963501");
        phoneList.add("16534348434");
        phoneList.add("18428637462");
        phoneList.add("13690470615");
        phoneList.add("15539613975");
        phoneList.add("17816115082");
        phoneList.add("14529464431");
        phoneList.add("19153650733");
        phoneList.add("16479429004");
        phoneList.add("15340613814");
        phoneList.add("19119950794");
        phoneList.add("18122332823");
        phoneList.add("17882324598");
        phoneList.add("13094566759");

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

    /**
     * 形式：15837312345,13737312345,2017-01-09 08:09:10,0360
     */
    public String product() {
        String caller = null;
        String callee = null;

        String callerName = null;
        String calleeName = null;

        //取得主叫电话号码
        int callerIndex = (int) (Math.random() * phoneList.size());

        /*Random random =new Random();
        random.nextInt(27); // 0 -26*/
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);
        while (true) {
            //取得被叫电话号码
            int calleeIndex = (int) (Math.random() * phoneList.size());
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);
            if (!caller.equals(callee)) break;
        }

        String buildTime = randomBuildTime(startTime, endTime);
        //0000
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format((int) (30 * 60 * Math.random()));
        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);
        return sb.toString();
    }

    /**
     * 根据传入的时间区间，在此范围内随机通话建立的时间
     * startTimeTS + (endTimeTs - startTimeTs) * Math.random();
     *
     * @param startTime
     * @param endTime
     */
    public String randomBuildTime(String startTime, String endTime) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) return null;

            long randomTS = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());
            Date resultDate = new Date(randomTS);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String resultTimeString = sdf2.format(resultDate);
            return resultTimeString;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将数据写入到文件中
     */
    public void writeLog(String filePath) {
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8");
            while (true) {
                Thread.sleep(500);
                String log = product();
                System.out.println(log);
                osw.write(log + "\n");
                //一定要手动flush才可以确保每条数据都写入到文件一次
                osw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e2) {
            e2.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args == null || args.length <= 0) {
            System.out.println("no arguments");
            return;
        }
        //String logPath = "D:\\calllog.csv";
        ProductLog productLog = new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);
    }
}
