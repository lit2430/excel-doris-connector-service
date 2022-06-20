package com.bigdata.util;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lit
 * @email
 * @date 2021/1/13 19:33
 * @description:
 **/

public class JdbcUtil {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtil.class);

    /**
     * @author: wudi
     * @date: 2021/1/13 19:44
     * @description: 执行sql 语句（单次执行）
     * @param hostUrl: 10.220.146.11:8040
     * @param db: ods
     * @param user: xxx
     * @param password: xxx
     * @param sql:  select 1
     * @return: java.util.List<com.alibaba.fastjson.JSONObject>
     */
    public static List<JSONObject> executeQuery(String hostUrl,String db,String user,String password,String sql){
        List<JSONObject> beJson = new ArrayList<>();
        String connectionUrl = String.format("jdbc:mysql://%s/%s",hostUrl,db);
        logger.info("加载到的数据源地址为：{}",connectionUrl);
        String jdbcDriverName = "com.mysql.cj.jdbc.Driver";
        Connection con = null;
        try {

            Class.forName(jdbcDriverName);
            con = DriverManager.getConnection(connectionUrl,user,password);
            PreparedStatement ps = con.prepareStatement(sql);
            long start = System.currentTimeMillis();
            ResultSet rs = ps.executeQuery();
            long end = System.currentTimeMillis();
            logger.info("SQL执行耗时:{}ms",(end-start));
            beJson = resultSetToJson(rs);
        } catch (SQLException e) {
            logger.error("SQL执行异常",e);
        } catch (Exception e) {
            logger.error("SQL执行错误",e);
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                logger.warn("连接关闭错误",e);
            }
        }
        return beJson;
    }


    /**
     * resultSet转List
     * @param rs
     * @return
     * @throws SQLException
     */
    public static List<JSONObject> resultSetToJson(ResultSet rs) throws SQLException
    {
        //定义接收的集合
        List<JSONObject> list = new ArrayList<JSONObject>();
        // 获取列数
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        // 遍历ResultSet中的每条数据
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName =metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            list.add(jsonObj);
        }
        return list;
    }
}
