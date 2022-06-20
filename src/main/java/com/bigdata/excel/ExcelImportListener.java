package com.bigdata.excel;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.doris.StreamLoadUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.text.SimpleDateFormat;
import java.util.*;


public class ExcelImportListener<T> extends AnalysisEventListener<T> {

    private static final Logger logger = LoggerFactory.getLogger(ExcelImportListener.class);

    private List<JSONObject> excelList = new ArrayList<>();

    private StreamLoadUtil streamLoadUtil;

    private String dbName; //库名
    private String tableName; //表名
    private Integer batchSize; //streamLoad批次大小
    private List<String> columns; //字段列表
    private Map<Integer, String> headMap;  //excel头map
    private Map<String, String> loadMap; //导入字段配置

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public ExcelImportListener(StreamLoadUtil streamLoadUtil, String dbName, String tableName, Integer batchSize, List<String> columns, Map<String, String> loadMap) {
        this.streamLoadUtil = streamLoadUtil;
        this.dbName = dbName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.columns = columns;
        this.loadMap = loadMap;
    }


    /**
     * 这个每一条数据解析都会来调用
     *
     * @param excel
     * @param analysisContext
     */
    @Override
    public void invoke(T excel, AnalysisContext analysisContext) {
        Map<String, String> data = transData(excel);
        JSONObject jsonData = JSONObject.parseObject(JSON.toJSONString(data));
        excelList.add(jsonData);
        //超过设置的batch就导入
        if (excelList.size() >= batchSize) {
            Boolean flag = streamLoadUtil.loadData(dbName, tableName, JSON.toJSONString(excelList), loadMap);
            if (!flag) {
                throw new RuntimeException("streamload导入数据失败");
            }
            excelList.clear();
        }
    }

    private Map<String, String> transData(T excel) {
        if (excel instanceof Map) {
            Map<String, String> result = new HashMap<>();
            Map<Integer, String> excelData = (Map<Integer, String>) excel;
            for (Map.Entry<Integer, String> entry : excelData.entrySet()) {
                String columnName = headMap.get(entry.getKey());
                result.put(columnName, entry.getValue());
            }
            if (columns.contains("rq") || columns.contains("updateStamp")){
                result.put("rq",sdf.format(new Date()));
                result.put("updateStamp",sdf.format(new Date()));
            }
            return result;
        } else {
            throw new RuntimeException("Excel格式错误");
        }
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        logger.info("最后批次导入数据量为：{}",excelList.size());
        streamLoadUtil.loadData(dbName, tableName, JSON.toJSONString(excelList), loadMap);
        logger.info("所有数据加载完毕");
    }

    /**
     * 这里会一行行的返回头
     *
     * @param headMap
     * @param context
     */
    @Override
    public void invokeHeadMap(Map<Integer, String> headMap, AnalysisContext context) {
        logger.info("解析到一条头数据:{}", JSON.toJSONString(headMap));
        if (columns == null) {
            throw new RuntimeException("表字段获取失败");
        }
        this.headMap = headMap;
    }

    /**
     * 在转换异常 获取其他异常下会调用本接口。抛出异常则停止读取。如果这里不抛出异常则 继续读取下一行。
     *
     * @param exception
     * @param context
     * @throws Exception
     */
    @Override
    public void onException(Exception exception, AnalysisContext context) {
        logger.error("解析失败，终止解析", exception.getMessage());
        String errorMsg = exception.getMessage();
        if (StringUtils.isBlank(errorMsg)) {
            errorMsg = "Excel解析失败";
        }
        throw new RuntimeException(errorMsg);
    }


}
