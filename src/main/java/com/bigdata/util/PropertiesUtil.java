package com.bigdata.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by:
 *
 * @Author: lit
 * @Date: 2022/01/12/14:48
 * @Description:
 */
public class PropertiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Map<String, String> getConfig(String filePath) {
        HashMap<String, String> configMap = new HashMap<>();
        Properties properties = new Properties();
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(filePath);
            br = new BufferedReader(fr);
            properties.load(br);
            properties.forEach((key, value) -> configMap.put(key.toString(), value.toString()));
            return configMap;
        } catch (Exception e) {
            logger.error("读取配置文件{}失败：{}", filePath, e.getMessage());
            return null;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
