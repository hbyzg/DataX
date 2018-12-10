package com.alibaba.datax.plugin.reader.hbase132reader;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

/**
 * @author jack_yang
 * @date 2018/12/3
 */
public enum ColumnType {
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date"),
    STRING("string"),
    BINARY_STRING("binarystring")
    ;

    private String typeName;
    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName){
        if(StringUtils.isBlank(typeName)){
            throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE,
                    String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE,
                String.format("Hbasereader 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }
    @Override
    public String toString() {
        return this.typeName;
    }


}
