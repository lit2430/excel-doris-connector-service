package com.bigdata.executor;

import com.alibaba.excel.EasyExcel;
import com.alibaba.fastjson.JSON;
import com.bigdata.excel.ExcelImportListener;
import com.google.common.collect.Lists;
import com.bigdata.doris.StreamLoadUtil;
import com.bigdata.exception.ReadPropertiesException;
import com.bigdata.util.PropertiesUtil;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by:
 *
 * @Author: lit
 * @Date: 2022/06/20/9:52
 * @Description:
 */
public class ExcelToDorisExecutor {

    //fe ip
    private static String ip;

    //fe http port default:8030
    private static String httpPort;

    //fe query port default：9030
    private static String queryPort;

    //writer to database
    private static String db;

    //writer to table
    private static String table;

    //jdbc user
    private static String dorisUser;

    //jdbc pwd
    private static String dorisPwd;

    //streamLoad batch size
    private static String batchSize;


    public static void main(String[] args) {

        /**
         * properties config path
         */
        String propPath = args[0];
       // String propPath = "src/main/resources/application.properties";
        /**
         * upload file path
         */
        String filePath = args[1];
       // String filePath = "C:\\Users\\lit\\Desktop\\test.xlsx";

        start(propPath, filePath);
    }


    private static void start(String proPath, String filePath) {
        Map<String, String> config = PropertiesUtil.getConfig(proPath);
        if (Objects.isNull(config)) {
            throw new ReadPropertiesException("properties read failed,please check your properties path", filePath);
        }
        ip = config.get("doris.ip");
        httpPort = config.get("doris.httpPort");
        queryPort = config.get("doris.queryPort");
        db = config.get("doris.db");
        table = config.get("doris.table");
        dorisUser = config.get("doris.jdbc.username");
        dorisPwd = config.get("doris.jdbc.password");
        batchSize = config.get("doris.streamLoad.batch.size");

        StreamLoadUtil streamLoadUtil = new StreamLoadUtil(ip, httpPort, queryPort, db, table, dorisUser, dorisPwd);

        streamLoadUtil.init();

        List<String> tableColumn = streamLoadUtil.getTableColumn();

        FileInputStream fs;
        try {
            fs = new FileInputStream(filePath);
            ExcelImportListener listener = new ExcelImportListener(streamLoadUtil, db, table, Integer.valueOf(batchSize), tableColumn, loadConf(tableColumn));
            EasyExcel.read(fs, listener).sheet().doRead();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    private static Map<String, String> loadConf(List<String> tableColumn) {
        Map<String, String> loadMap = new HashMap<>();
        /**
         * 构建column
         */
        StringBuilder columnsSb = new StringBuilder();
        for (int i = 0; i < tableColumn.size(); i++) {
            if (i < tableColumn.size() - 1) {
                columnsSb.append(tableColumn.get(i)).append(",");
            } else {
                columnsSb.append(tableColumn.get(i));
            }
        }

        /**
         * 构建 jsonpath
         */

        List<Object> jsonpathList = Lists.newArrayList();
        for (int i = 0; i < tableColumn.size(); i++) {
            StringBuilder jsonpathSb = new StringBuilder();
            jsonpathSb.append("$.").append(tableColumn.get(i));
            jsonpathList.add(jsonpathSb.toString());
        }
        loadMap.put("columns", columnsSb.toString());
        loadMap.put("jsonpaths", JSON.toJSONString(jsonpathList));
        loadMap.put("max_filter_ratio", "1.0");
        loadMap.put("format", "json");
        loadMap.put("strip_outer_array", "true");
        return loadMap;
    }


}
