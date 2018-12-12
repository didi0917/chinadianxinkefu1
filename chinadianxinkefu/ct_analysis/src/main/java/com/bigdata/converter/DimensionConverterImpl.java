package com.bigdata.converter;


import com.bigdata.kv.base.BaseDimension;
import com.bigdata.kv.key.ContactDimension;
import com.bigdata.kv.key.DateDimension;
import com.bigdata.utils.JDBCInstance;
import com.bigdata.utils.JDBCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
 *
 *
 */
public class DimensionConverterImpl implements DimensionConverter {
    //Logger
    private static final Logger logger = LoggerFactory.getLogger(DimensionConverterImpl.class);
    //

    public DimensionConverterImpl() {

    }

    @Override
    public int getDimensionID(BaseDimension dimension) {

        String[] sqls = null;
        //如果传入的对象是DateDimension
        if (dimension instanceof DateDimension) {

            sqls = getDateDimensionSQL();
        } else if (dimension instanceof ContactDimension) {
            sqls = getContactDimensionSQL();
        } else {
            throw new RuntimeException("没有匹配到对应维度信息.");
        }

        //准备对Mysql表进行操作，先查询，有可能再插入
        Connection conn = this.getConnection();
        int id = -1;
        synchronized (this) {
            id = execSQL(conn, sqls, dimension);
        }
        //将刚查询到的id加入到缓存中
        //lruCache.put(cacheKey, id);
        return id;
    }

    /**
     * 得到当前线程维护的Connection对象
     * @return
     */
    private Connection getConnection() {
        Connection conn = null;
        try {

            if (conn == null || conn.isClosed()) {
                conn = JDBCInstance.getInstance();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     *
     * @param conn JDBC连接器
     * @param sqls 长度为2，第一个位置为查询语句，第二个位置为插入语句
     * @param dimension 对应维度所保存的数据
     * @return
     */
    private int execSQL(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //1
            //查询的preparedStatement    先查询
            preparedStatement = conn.prepareStatement(sqls[0]);
            //根据不同的维度，封装不同的SQL语句
            setArguments(preparedStatement, dimension);
            //执行查询 得到结果集
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                int result = resultSet.getInt(1);
                //释放资源
                JDBCUtil.close(null, preparedStatement, resultSet);
                return result;
            }
            //释放资源
            JDBCUtil.close(null, preparedStatement, resultSet);

            //2
            //  查询后如果没有就执行插入，封装插入的sql语句
                       preparedStatement = conn.prepareStatement(sqls[1]);
            setArguments(preparedStatement, dimension);
            //执行插入
            preparedStatement.executeUpdate();
            //释放资源
            JDBCUtil.close(null, preparedStatement, null);

            //3
            //查询的preparedStatement
            preparedStatement = conn.prepareStatement(sqls[0]);
            //根据不同的维度，封装不同的SQL语句
            setArguments(preparedStatement, dimension);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            //释放资源
            JDBCUtil.close(null, preparedStatement, resultSet);
        }
        return -1;
    }

    /**
     * 设置SQL语句的具体参数
     * @param preparedStatement
     * @param dimension
     */
    private void setArguments(PreparedStatement preparedStatement, BaseDimension dimension) {
        int i = 0;
        try {
            if(dimension instanceof DateDimension){
                //可以优化
                //对日期表进行封装参数 用多态有什么好处 丰富了参数类型 解耦一些类（一个类继承一个类，那么他就不能继承另一个类，使用接口后就可以）
                DateDimension dateDimension = (DateDimension) dimension;
                preparedStatement.setString(++i, dateDimension.getYear());
                preparedStatement.setString(++i, dateDimension.getMonth());
                preparedStatement.setString(++i, dateDimension.getDay());
            }else if(dimension instanceof ContactDimension){
                ContactDimension contactDimension = (ContactDimension) dimension;
                preparedStatement.setString(++i, contactDimension.getTelephone());
                preparedStatement.setString(++i, contactDimension.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回联系人表的查询和插入语句
     *
     * @return
     */
    private String[] getContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 返回时间表的查询和插入语句
     *
     * @return
     */
    private String[] getDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }


}
