package com.bigdata.doris;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.constants.RespContent;
import com.bigdata.exception.DorisImportException;
import com.bigdata.util.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @description: Stream Load工具类
 * @author: casey
 * @date: 2021-03-11 16:44
 **/

public class StreamLoadUtil {

    private static final Logger logger = LoggerFactory.getLogger(StreamLoadUtil.class);

    // 请求成功编码
    private static final Integer SUCCESS_CODE = 200;

    // 请求成功状态
    private static final List<String> SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));

    // 重试次数
    private static final Integer retries = 5;

    // 重试间隔
    private static final Integer retryInterval = 5000;

    // beMap
    private static Map<String, Integer> beMap;


    private String ip;
    private String httpPort;
    private String queryPort;
    private String db;
    private String table;
    private String dorisUser;
    private String dorisPwd;

    public StreamLoadUtil(String ip, String httpPort, String queryPort, String db, String table, String dorisUser, String dorisPwd) {
        this.ip = ip;
        this.httpPort = httpPort;
        this.queryPort = queryPort;
        this.db = db;
        this.table = table;
        this.dorisUser = dorisUser;
        this.dorisPwd = dorisPwd;
    }


    public void init() {
        try {
            beMap = getBeConfig();
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Get BE alive nodes failed");
        }
    }


    /**
     * 获取Be配置
     */
    public Map<String, Integer> getBeConfig() throws SQLException {
        Map<String, Integer> beMap = new HashMap<>();
        List<JSONObject> backends = JdbcUtil.executeQuery(ip + ":" + queryPort, db, dorisUser, dorisPwd, "show backends");
        for (JSONObject be : backends) {
            if (be.getBoolean("Alive")) {
                String ip = be.getString("IP");
                String port = be.getString("HttpPort");
                beMap.put(ip + ":" + port, 1);
            }
        }
        logger.info("BE 配置:{}", beMap);
        return beMap;
    }


    /**
     * 获取表字段信息
     */
    public List<String> getTableColumn() {
        String sql = "select COLUMN_NAME from information_schema.`columns` where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' ";
        String formatSql = String.format(sql, db, table);
        List<JSONObject> columnsJobj = JdbcUtil.executeQuery(ip + ":" + queryPort, db, dorisUser, dorisPwd, formatSql);
        List<String> columns = columnsJobj.stream().map(obj -> obj.getString("COLUMN_NAME")).collect(Collectors.toList());
        logger.info("{}表的字段列表 : {}", table, columns);
        return columns;
    }


    /**
     * 导入数据
     *
     * @param data
     */
    public Boolean loadData(String dorisDb, String dorisTable, String data, Map<String, String> loadConf) {
        RespContent errorResp = null;
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(String.format("%s:%s", ip, httpPort), dorisDb, dorisTable, dorisUser, dorisPwd, loadConf, beMap);
        boolean flag = false;
        for (int i = 1; i <= retries; i++) {
            if (i > 1) {
                logger.info(String.format("%s重试第%s次", dorisTable, i));
            }
            try {
                DorisStreamLoad.LoadResponse loadResponse2 = dorisStreamLoad.loadBatch(data);
                RespContent respContent2 = JSON.parseObject(loadResponse2.respContent, RespContent.class);
                if (SUCCESS_STATUS.contains(respContent2.getStatus())) {
                    logger.info(String.format("%s导入成功", dorisTable));
                    flag = true;
                    return flag;
                } else {
                    errorResp = respContent2;
                }
            } catch (Exception ex) {
                logger.error(String.format("%s重试第%s次失败，原因：%s", dorisTable, i, ex));
                // 重试失败之后等5秒再试
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //重试5次未成功
        if (!flag) {
            logger.error("导入doris失败.");
            throw new DorisImportException("导入doris失败", errorResp);
        }
        return false;
    }
}