package com.alibaba.datax.plugin.reader.hbase132reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jack_yang
 * @date 2018/12/3
 */
public class Hbase132Helper {
    private static final Logger LOG = LoggerFactory.getLogger(Hbase132Helper.class);

    public static List<Configuration> split(Configuration configuration) {
        byte[] startRowkeyByte = Hbase132Helper.convertUserStartRowkey(configuration);
        byte[] endRowkeyByte = Hbase132Helper.convertUserEndRowkey(configuration);

        if (startRowkeyByte.length != 0 && endRowkeyByte.length != 0
                && Bytes.compareTo(startRowkeyByte, endRowkeyByte) > 0) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 中 startRowkey 不得大于 endRowkey.");
        }
        RegionLocator regionLocator = Hbase132Helper.getRegionLocator(configuration);
        List<Configuration> resultConfigurations ;
        try {
            Pair<byte[][], byte[][]> regionRanges = regionLocator.getStartEndKeys();
            if (null == regionRanges) {
                throw DataXException.asDataXException(Hbase132ReaderErrorCode.SPLIT_ERROR, "获取源头 Hbase 表的 rowkey 范围失败.");
            }
            resultConfigurations = Hbase132Helper.doSplit(configuration, startRowkeyByte, endRowkeyByte,
                    regionRanges);

            LOG.info("HBaseReader split job into {} tasks.", resultConfigurations.size());
            return resultConfigurations;
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.SPLIT_ERROR, "切分源头 Hbase 表失败.", e);
        }finally {
            Hbase132Helper.closeRegionLocator(regionLocator);
        }

    }
    public static void closeRegionLocator(RegionLocator regionLocator){
        try {
            if(null != regionLocator) {
                regionLocator.close();
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.CLOSE_HBASE_REGINLOCTOR_ERROR, e);
        }
    }
    private static List<Configuration> doSplit(Configuration config, byte[] startRowkeyByte,
                                               byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges) {

        List<Configuration> configurations = new ArrayList<Configuration>();

        for (int i = 0; i < regionRanges.getFirst().length; i++) {

            byte[] regionStartKey = regionRanges.getFirst()[i];
            byte[] regionEndKey = regionRanges.getSecond()[i];

            // 当前的region为最后一个region
            // 如果最后一个region的start Key大于用户指定的userEndKey,则最后一个region，应该不包含在内
            // 注意如果用户指定userEndKey为"",则此判断应该不成立。userEndKey为""表示取得最大的region
            if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0
                    && (endRowkeyByte.length != 0 && (Bytes.compareTo(
                            regionStartKey, endRowkeyByte) > 0))) {
                continue;
            }

            // 如果当前的region不是最后一个region，
            // 用户配置的userStartKey大于等于region的endkey,则这个region不应该含在内
            if ((Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0)
                    && (Bytes.compareTo(startRowkeyByte, regionEndKey) >= 0)) {
                continue;
            }

            // 如果用户配置的userEndKey小于等于 region的startkey,则这个region不应该含在内
            // 注意如果用户指定的userEndKey为"",则次判断应该不成立。userEndKey为""表示取得最大的region
            if (endRowkeyByte.length != 0
                    && (Bytes.compareTo(endRowkeyByte, regionStartKey) <= 0)) {
                continue;
            }

            Configuration p = config.clone();

            String thisStartKey = getStartKey(startRowkeyByte, regionStartKey);

            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey);

            p.set(Key.START_ROWKEY, thisStartKey);
            p.set(Key.END_ROWKEY, thisEndKey);

            LOG.debug("startRowkey:[{}], endRowkey:[{}] .", thisStartKey, thisEndKey);

            configurations.add(p);
        }

        return configurations;
    }
    private static String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey) {
        if (startRowkeyByte == null) {// 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException(
                    "userStartKey should not be null!");
        }

        byte[] tempStartRowkeyByte;

        if (Bytes.compareTo(startRowkeyByte, regionStarKey) < 0) {
            tempStartRowkeyByte = regionStarKey;
        } else {
            tempStartRowkeyByte = startRowkeyByte;
        }
        return Bytes.toStringBinary(tempStartRowkeyByte);
    }
    private static String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey) {
        if (endRowkeyByte == null) {// 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException("userEndKey should not be null!");
        }

        byte[] tempEndRowkeyByte;

        if (endRowkeyByte.length == 0) {
            tempEndRowkeyByte = regionEndKey;
        } else if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
            // 为最后一个region
            tempEndRowkeyByte = endRowkeyByte;
        } else {
            if (Bytes.compareTo(endRowkeyByte, regionEndKey) > 0) {
                tempEndRowkeyByte = regionEndKey;
            } else {
                tempEndRowkeyByte = endRowkeyByte;
            }
        }

        return Bytes.toStringBinary(tempEndRowkeyByte);
    }

    public static byte[] convertInnerStartRowkey(Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(startRowkey);
    }

    public static byte[] convertInnerEndRowkey(Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        return Bytes.toBytesBinary(endRowkey);
    }
    public static Connection getHbaseConnection(String hbaseConfig) {
        if (StringUtils.isBlank(hbaseConfig)) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.REQUIRED_VALUE, "读 Hbase 时需要配置hbaseConfig，其内容为 Hbase 连接信息，请联系 Hbase PE 获取该信息.");
        }
        org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
        try {
            Map<String, String> hbaseConfigMap = JSON.parseObject(hbaseConfig, new TypeReference<Map<String, String>>() {});
            // 用户配置的 key-value 对 来表示 hbaseConfig
            Validate.isTrue(hbaseConfigMap != null && hbaseConfigMap.size() !=0, "hbaseConfig不能为空Map结构!");
            for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
                hConfiguration.set(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.GET_HBASE_CONNECTION_ERROR, e);
        }
        Connection hConnection = null;
        try {
            hConnection = ConnectionFactory.createConnection(hConfiguration);

        } catch (Exception e) {
            Hbase132Helper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.GET_HBASE_CONNECTION_ERROR, e);
        }
        return hConnection;
    }
    public static boolean isRowkeyColumn(String columnName) {
        return Constant.ROWKEY_FLAG.equalsIgnoreCase(columnName);
    }

    /**
     *
     * @param hConnection
     */
    public static void closeConnection(Connection hConnection){
        try {
            if(null != hConnection) {
                hConnection.close();
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.CLOSE_HBASE_CONNECTION_ERROR, e);
        }
    }
    public static RegionLocator getRegionLocator(com.alibaba.datax.common.util.Configuration configuration){
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        Connection hConnection = Hbase132Helper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        Admin admin = null;
        RegionLocator regionLocator = null;
        try {
            admin = hConnection.getAdmin();
            Hbase132Helper.checkHbaseTable(admin,hTableName);
            regionLocator = hConnection.getRegionLocator(hTableName);
        } catch (Exception e) {
            Hbase132Helper.closeRegionLocator(regionLocator);
            Hbase132Helper.closeAdmin(admin);
            Hbase132Helper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.GET_HBASE_REGINLOCTOR_ERROR, e);
        }
        return regionLocator;

    }
    public static void closeAdmin(Admin admin){
        try {
            if(null != admin) {
                admin.close();
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.CLOSE_HBASE_ADMIN_ERROR, e);
        }
    }
    public static void closeTable(Table table){
        try {
            if(null != table) {
                table.close();
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.CLOSE_HBASE_TABLE_ERROR, e);
        }
    }

    public static void closeResultScanner(ResultScanner resultScanner){
        if(null != resultScanner) {
            resultScanner.close();
        }
    }

    public static  void checkHbaseTable(Admin admin, TableName hTableName) throws IOException {
        if(!admin.tableExists(hTableName)){
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "HBase源头表" + hTableName.toString()
                    + "不存在, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(!admin.isTableAvailable(hTableName)){
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "HBase源头表" +hTableName.toString()
                    + " 不可用, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
        if(admin.isTableDisabled(hTableName)){
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "HBase源头表" +hTableName.toString()
                    + "is disabled, 请检查您的配置 或者 联系 Hbase 管理员.");
        }
    }
    public static byte[] convertUserStartRowkey(com.alibaba.datax.common.util.Configuration configuration) {
        String startRowkey = configuration.getString(Key.START_ROWKEY);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);
            return Hbase132Helper.stringToBytes(startRowkey, isBinaryRowkey);
        }
    }

    public static Table getTable(com.alibaba.datax.common.util.Configuration configuration){
        String hbaseConfig = configuration.getString(Key.HBASE_CONFIG);
        String userTable = configuration.getString(Key.TABLE);
        Connection hConnection = Hbase132Helper.getHbaseConnection(hbaseConfig);
        TableName hTableName = TableName.valueOf(userTable);
        Admin admin = null;
        Table hTable = null;
        try {
            admin = hConnection.getAdmin();
            Hbase132Helper.checkHbaseTable(admin,hTableName);
            hTable = hConnection.getTable(hTableName);

        } catch (Exception e) {
            Hbase132Helper.closeTable(hTable);
            Hbase132Helper.closeAdmin(admin);
            Hbase132Helper.closeConnection(hConnection);
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.GET_HBASE_TABLE_ERROR, e);
        }
        return hTable;
    }
    //将多竖表column变成<familyQualifier,<>>形式
    public static HashMap<String,HashMap<String,String>> parseColumnOfMultiversionMode(List<Map> column){

        HashMap<String,HashMap<String,String>> familyQualifierMap = new HashMap<String,HashMap<String,String>>();
        for (Map<String, String> aColumn : column) {
            String type = aColumn.get(Key.TYPE);
            String columnName = aColumn.get(Key.NAME);
            String dateformat = aColumn.get(Key.FORMAT);

            ColumnType.getByTypeName(type);
            Validate.isTrue(StringUtils.isNotBlank(columnName), "Hbasereader 中，column 需要配置列名称name,格式为 列族:列名，您的配置为空,请检查并修改.");

            String familyQualifier;
            if( !Hbase132Helper.isRowkeyColumn(columnName)){
                String[] cfAndQualifier = columnName.split(":");
                if ( cfAndQualifier.length != 2) {
                    throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + columnName);
                }
                familyQualifier = StringUtils.join(cfAndQualifier[0].trim(),":",cfAndQualifier[1].trim());
            }else{
                familyQualifier = columnName.trim();
            }

            HashMap<String,String> typeAndFormat = new  HashMap<String,String>();
            typeAndFormat.put(Key.TYPE,type);
            typeAndFormat.put(Key.FORMAT,dateformat);
            familyQualifierMap.put(familyQualifier,typeAndFormat);
        }
        return familyQualifierMap;
    }

    /**
     * 用于解析 Normal 模式下的列配置
     */
    public static List<HbaseColumnCell> parseColumnOfNormalMode(List<Map> column) {
        List<HbaseColumnCell> hbaseColumnCells = new ArrayList<HbaseColumnCell>();

        HbaseColumnCell oneColumnCell;

        for (Map<String, String> aColumn : column) {
            ColumnType type = ColumnType.getByTypeName(aColumn.get(Key.TYPE));
            String columnName = aColumn.get(Key.NAME);
            String columnValue = aColumn.get(Key.VALUE);
            String dateformat = aColumn.get(Key.FORMAT);

            if (type == ColumnType.DATE) {

                if(dateformat == null){
                    dateformat = Constant.DEFAULT_DATA_FORMAT;
                }
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "Hbasereader 在 normal 方式读取时则要么是 type + name + format 的组合，要么是type + value + format 的组合. 而您的配置非这两种组合，请检查并修改.");

                oneColumnCell = new HbaseColumnCell
                        .Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .dateformat(dateformat)
                        .build();
            } else {
                Validate.isTrue(StringUtils.isNotBlank(columnName) || StringUtils.isNotBlank(columnValue), "Hbasereader 在 normal 方式读取时，其列配置中，如果类型不是时间，则要么是 type + name 的组合，要么是type + value 的组合. 而您的配置非这两种组合，请检查并修改.");
                oneColumnCell = new HbaseColumnCell.Builder(type)
                        .columnName(columnName)
                        .columnValue(columnValue)
                        .build();
            }

            hbaseColumnCells.add(oneColumnCell);
        }

        return hbaseColumnCells;
    }
    public static byte[] convertUserEndRowkey(com.alibaba.datax.common.util.Configuration configuration) {
        String endRowkey = configuration.getString(Key.END_ROWKEY);
        if (StringUtils.isBlank(endRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY);
            return Hbase132Helper.stringToBytes(endRowkey, isBinaryRowkey);
        }
    }

    private static byte[] stringToBytes(String rowkey, boolean isBinaryRowkey) {
        if (isBinaryRowkey) {
            return Bytes.toBytesBinary(rowkey);
        } else {
            return Bytes.toBytes(rowkey);
        }
    }

}
