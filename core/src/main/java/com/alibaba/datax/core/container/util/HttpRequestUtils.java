package com.alibaba.datax.core.container.util;




import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author  jack yang
 * @date  2018-01-15
 */
public class HttpRequestUtils {
    /**
     *日志记录
     */
    private static Logger logger = LoggerFactory.getLogger(HttpRequestUtils.class);
    public static String  Get(String url,Map<String,String> headerMap) {
        String strResult ="";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpget = new HttpGet(url);
            if(headerMap != null && !headerMap.isEmpty()){
                List<String> keyList = new ArrayList<String>(headerMap.keySet());
                for(String key: keyList){
                    httpget.setHeader(key,headerMap.get(key));
                }
            }
            CloseableHttpResponse response = httpclient.execute(httpget);
            int success =200;
            /**请求发送成功，并得到响应**/
            if (response.getStatusLine().getStatusCode() ==success) {
                  strResult = EntityUtils.toString(response.getEntity());

            } else {
                logger.error("get请求提交失败:" + url);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭连接,释放资源
            try {
                httpclient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return strResult;
    }



    /**
     * 发送 get请求
     * @param url
     * @return
     */
    public static JSONObject httpGet(String url, Map<String,String> headerMap) {
        JSONObject jsonResult = null;
        jsonResult = JSONObject.parseObject(Get(url,headerMap));
        return jsonResult;
    }

    /**
     * post请求
     * @param url         url地址
     * @param jsonParam     参数
     * @return
     */
    public static JSONObject httpPost(String url, JSONObject jsonParam){
        CloseableHttpClient httpclient = HttpClients.createDefault();
        JSONObject jsonResult = null;
        HttpPost httppost = new HttpPost(url);
        try {
            if (null != jsonParam) {
                //解决中文乱码问题
                StringEntity entity = new StringEntity(jsonParam.toString(), "utf-8");
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");

                httppost.setEntity(entity);
            }
            CloseableHttpResponse response = httpclient.execute(httppost);
            url = URLDecoder.decode(url, "UTF-8");
            int success =200;
            if (response.getStatusLine().getStatusCode() == success) {
                try {
                    String str = EntityUtils.toString(response.getEntity());
                    jsonResult = JSONObject.parseObject(str);
                } catch (Exception e) {
                    logger.error("post请求提交失败:" + url, e);
                }
            }
        } catch (IOException e) {
            logger.error("post请求提交失败:" + url, e);
        }
        return jsonResult;
    }

    /**
     * post请求
     * @param url         url地址
     * @param params     参数
     * @return
     */
    public static JSONObject httpPost(String url, Map<String,String> params, Map<String,String> headerMap){
        CloseableHttpClient httpclient = HttpClients.createDefault();
        JSONObject jsonResult = null;
        HttpPost httppost = new HttpPost(url);
        List<NameValuePair> param = new ArrayList<NameValuePair>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            param.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        if(headerMap != null && !headerMap.isEmpty()){
            List<String> keyList = new ArrayList<String>(headerMap.keySet());
            for(String key: keyList){
                httppost.setHeader(key,headerMap.get(key));
            }
        }
        try {
            httppost.setEntity(new UrlEncodedFormEntity(param, "utf-8"));
            CloseableHttpResponse response = httpclient.execute(httppost);
            HttpEntity httpEntity = response.getEntity();
            //取出应答字符串
            String str = EntityUtils.toString(httpEntity);
            logger.info("resultString:"+str);
            jsonResult = JSONObject.parseObject(str);
        } catch (Exception e) {
            logger.error("post请求提交失败:" + url, e);
        }
        return jsonResult;
    }
    /**
     * post请求
     * @param url         url地址
     * @param params     参数
     * @return
     */
    public static JSONObject httpPostObject(String url, Map<String,Object> params, Map<String,String> headerMap){
        CloseableHttpClient httpclient = HttpClients.createDefault();
        JSONObject jsonResult = null;
        HttpPost httppost = new HttpPost(url);
        List<NameValuePair> param = new ArrayList<NameValuePair>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {

            param.add(new BasicNameValuePair(entry.getKey(), String.valueOf(entry.getValue())));
        }
        if(headerMap != null && !headerMap.isEmpty()){
            List<String> keyList = new ArrayList<String>(headerMap.keySet());
            for(String key: keyList){
                httppost.setHeader(key,headerMap.get(key));
            }
        }
        try {
            httppost.setEntity(new UrlEncodedFormEntity(param, "utf-8"));
            CloseableHttpResponse response = httpclient.execute(httppost);
            HttpEntity httpEntity = response.getEntity();
            //取出应答字符串
            String str = EntityUtils.toString(httpEntity);
            logger.info("resultString:"+str);
            jsonResult = JSONObject.parseObject(str);
        } catch (Exception e) {
            logger.error("post请求提交失败:" + url, e);
        }
        return jsonResult;
    }

    /**
     *   post请求
     * @param url  url地址
     * @param params  参数
     * @param headerMap  头
     * @param charset 编码  utf-8  或 ascii
     * @return
     */
    public static JSONObject httpPost(String url, Map<String,String> params, Map<String,String> headerMap, String charset){
        CloseableHttpClient httpclient = HttpClients.createDefault();
        JSONObject jsonResult = null;
        HttpPost httppost = new HttpPost(url);
        List<NameValuePair> param = new ArrayList<NameValuePair>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            param.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        if(headerMap != null && !headerMap.isEmpty()){
            List<String> keyList = new ArrayList<String>(headerMap.keySet());
            for(String key: keyList){
                httppost.setHeader(key,headerMap.get(key));
            }
        }
        try {
            httppost.setEntity(new UrlEncodedFormEntity(param, charset));
            CloseableHttpResponse response = httpclient.execute(httppost);
            HttpEntity httpEntity = response.getEntity();
            //取出应答字符串
            String str = EntityUtils.toString(httpEntity);
            logger.info("resultString:"+str);
            jsonResult = JSONObject.parseObject(str);
        } catch (Exception e) {
            logger.error("post请求提交失败:" + url, e);
        }
        return jsonResult;
    }
    /**
     * 数据流post请求
     * @param urlStr
     * @param strInfo
     */
    public static String doPost(String urlStr, String strInfo) {
        String reStr="";
        try {
            URL url = new URL(urlStr);
            URLConnection con = url.openConnection();
            con.setDoOutput(true);
            con.setRequestProperty("Pragma:", "no-cache");
            con.setRequestProperty("Cache-Control", "no-cache");
            con.setRequestProperty("Content-Type", "text/xml");
            OutputStreamWriter out = new OutputStreamWriter(con.getOutputStream());
            out.write(new String(strInfo.getBytes("utf-8")));
            out.flush();
            out.close();
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
            String line = "";
            for (line = br.readLine(); line != null; line = br.readLine()) {
                reStr += line;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reStr;
    }
    //处理http请求  requestUrl  requestMethod请求方式，值为"GET"或"POST"

    /**
     * 处理http请求
     * @param requestUrl  请求地址
     * @param requestMethod  请求方式，值为"GET"或"POST"
     * @param outputStr   发起http请求需要带的参数
     * @return
     */
    public static String httpRequest(String requestUrl,String requestMethod,String outputStr){
        StringBuffer buffer=null;
        try{
            URL url=new URL(requestUrl);
            HttpURLConnection conn=(HttpURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod(requestMethod);
            conn.connect();
            //往服务器端写内容 也就是发起http请求需要带的参数
            if(null!=outputStr){
                OutputStream os=conn.getOutputStream();
                os.write(outputStr.getBytes("utf-8"));
                os.close();
            }

            //读取服务器端返回的内容
            InputStream is=conn.getInputStream();
            InputStreamReader isr=new InputStreamReader(is,"utf-8");
            BufferedReader br=new BufferedReader(isr);
            buffer=new StringBuffer();
            String line=null;
            while((line=br.readLine())!=null){
                buffer.append(line);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return buffer.toString();
    }
    /**
     * 字符串转hex字符串
     * @throws UnsupportedEncodingException
     */
    public static String strToHex(String str) throws UnsupportedEncodingException {
        return String.format("%x", new BigInteger(1, str.getBytes("UTF-8")));
    }


}
