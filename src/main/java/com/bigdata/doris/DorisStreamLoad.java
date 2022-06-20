package com.bigdata.doris;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

/**
 * @author
 * @email
 * @date 2022/5/13 11:23
 * @description: doris stream load utils
 **/

public class DorisStreamLoad implements Serializable{

    private static final Logger logger = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private String hostPort;
    private String db;
    private String tbl;
    private String user;
    private String passwd;
    private Map<String, String> conf;
    private Map<String, Integer> beConf;



    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String passwd, Map<String, String> conf, Map<String,Integer> beConf) {
        this.hostPort = hostPort;
        this.db = db;
        this.tbl = tbl;
        this.user = user;
        this.passwd = passwd;
        this.conf = conf;
        this.beConf = beConf;
    }

    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String passwd, Map<String, String> conf) {
        this.hostPort = hostPort;
        this.db = db;
        this.tbl = tbl;
        this.user = user;
        this.passwd = passwd;
        this.conf = conf;
    }

    public static String getLoadUrlPattern() {
        return loadUrlPattern;
    }

    public static void setLoadUrlPattern(String loadUrlPattern) {
        DorisStreamLoad.loadUrlPattern = loadUrlPattern;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }

    public Map<String, Integer> getBeConf() {
        return beConf;
    }

    public void setBeConf(Map<String, Integer> beConf) {
        this.beConf = beConf;
    }


    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        //默认严格模式
//        conn.addRequestProperty("max_filter_ratio", "0");
//        conn.addRequestProperty("strict_mode", "true");
        //自定义参数
        if(ObjectUtils.isNotEmpty(conf)){
            for(Map.Entry<String, String> entry : conf.entrySet()){
                conn.addRequestProperty(entry.getKey(),entry.getValue());
            }
        }
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("status: ").append(status);
            sb.append(", resp msg: ").append(respMsg);
            sb.append(", resp content: ").append(respContent);
            return sb.toString();
        }
    }

    public LoadResponse loadBatch(String data) {
        if(ObjectUtils.isEmpty(beConf)){
            throw new RuntimeException("BE 配置为空");
        }
        String beLoadUrl = String.format(loadUrlPattern, BeUtil.getBeRoundRobin(beConf), db, tbl);
        Calendar calendar = Calendar.getInstance();
        String label = String.format("audit_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-",""));

        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        InputStream stream = null;
        try {
            int status;
            // build request and send to new be location
            beConn = getConnection(beLoadUrl, label);
            // send data to be
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            bos.write(data.getBytes());
            bos.close();

            // get respond
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            stream = (InputStream) beConn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            br.close();
            logger.info("AuditLoader plugin load with label: {}, response code: {}, msg: {}, content: {}",label, status, respMsg, response.toString());
            return new LoadResponse(status, respMsg, response.toString());

        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            logger.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        } finally {
            if(stream != null){
                try {
                    stream.close();
                } catch (IOException e) {
                    logger.error("stream关闭失败:",e);
                }
            }
            if (feConn != null) {
                feConn.disconnect();
            }
            if (beConn != null) {
                beConn.disconnect();
            }
        }
    }
}
