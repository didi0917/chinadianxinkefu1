package com.bigdata.outputformat;


import com.bigdata.converter.DimensionConverterImpl;
import com.bigdata.intimacy.Intimacy;
import com.bigdata.kv.key.ComDimension;
import com.bigdata.kv.value.CountDurationValue;
import com.bigdata.utils.JDBCInstance;
import com.bigdata.utils.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<ComDimension, CountDurationValue>{
    private OutputCommitter committer = null;

    @Override
    public RecordWriter<ComDimension, CountDurationValue> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        //初始化JDBC连接器对象
        Connection conn = null;
        conn = JDBCInstance.getInstance();
        try {
            // 通过设置该属性，启动手动事务
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

        return new MysqlRecordWriter(conn);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        //输出校检
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if(committer == null){
            String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = name == null ? null : new Path(name);
            committer = new FileOutputCommitter(outputPath, context);
        }
        return committer;
    }

    static class MysqlRecordWriter extends RecordWriter<ComDimension, CountDurationValue> {
        private DimensionConverterImpl dci = new DimensionConverterImpl();
        private Connection conn = null;
        private PreparedStatement preparedStatement = null;
        private String insertSQL = null;
        private int count = 0;
        private final int BATCH_SIZE = 500;
        public MysqlRecordWriter(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void write(ComDimension key, CountDurationValue value) throws IOException, InterruptedException {
            //上面两个参数是reduce端的输出
            try {
                //tb_call
                //id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum

                //year month day
                int idDateDimension = dci.getDimensionID(key.getDateDimension());
                //telephone name
                int idContactDimension = dci.getDimensionID(key.getContactDimension());

                String idDateContact = idDateDimension + "_" + idContactDimension;

                int callSum = Integer.valueOf(value.getCallSum());
                int callDurationSum = Integer.valueOf(value.getCallDurationSum());
                //计算两个联系人之间的亲密度 并将数据插入到tb_intimacy表中
                //亲密度的计算方式 这里设置为通过次数*通话时长
                   int intimacy = callDurationSum*callDurationSum;
                   Intimacy inti = new Intimacy();
                 //需要分别得到两个联系人的ID
                //现在得到一个人的ID如何得到另外一个人的Id
                //最终发现数据过来的时候是没有被叫信息的
                    //inti.insertToTableIntimacy(conn,inti,idContactDimension);
                //  一个人的基本信息  id
                //  年月日          id

                // DUPLICATE  duplicate
                if(insertSQL == null){
                    //mysql    获取到date表和联系人表的id后进行插入到tb_call表中
                    insertSQL = "INSERT INTO `tb_call` (`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ?;";
                }

                if(preparedStatement == null){
                    preparedStatement = conn.prepareStatement(insertSQL);
                }
                //本次SQL
                int i = 0;
                preparedStatement.setString(++i, idDateContact);
                preparedStatement.setInt(++i, idDateDimension);
                preparedStatement.setInt(++i, idContactDimension);
                preparedStatement.setInt(++i, callSum);
                preparedStatement.setInt(++i, callDurationSum);
                //无则插入，有则更新的判断依据
                preparedStatement.setString(++i, idDateContact);
                preparedStatement.addBatch();
                count++;
                if(count >= BATCH_SIZE){
                    preparedStatement.executeBatch();
                    conn.commit();
                    count = 0;
                    preparedStatement.clearBatch();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                if(preparedStatement != null){
                    preparedStatement.executeBatch();
                    this.conn.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                JDBCUtil.close(conn, preparedStatement, null);
            }
        }
    }
}
