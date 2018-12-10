package com.alibaba.datax.plugin.reader.hbase132reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author jack_yang
 * @date 2018/12/3
 */
public class Hbase132Reader extends Reader {

    public static class Job extends Reader.Job {
        private Configuration originConfig = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return Hbase132Helper.split(this.originConfig);
        }

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            validateParameter();
        }

        private void validateParameter() {


        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Reader.Task {
        private Configuration taskConfig;
        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private HbaseAbstractTask hbaseTaskProxy;

        @Override
        public void startRead(RecordSender recordSender) {
            this.taskConfig = super.getPluginJobConf();
            String mode = this.taskConfig.getString(Key.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);
            switch (modeType) {
                case Normal:
                    this.hbaseTaskProxy = new NormalTask(this.taskConfig);
                    break;
                case MultiVersionFixedColumn:
                    this.hbaseTaskProxy = new MultiVersionFixedColumnTask(this.taskConfig);
                    break;
                default:
                    throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持此类模式:" + modeType);
            }
        }

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            String mode = this.taskConfig.getString(Key.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);

            switch (modeType) {
                case Normal:
                    this.hbaseTaskProxy = new NormalTask(this.taskConfig);
                    break;
                case MultiVersionFixedColumn:
                    this.hbaseTaskProxy = new MultiVersionFixedColumnTask(this.taskConfig);
                    break;
                default:
                    throw DataXException.asDataXException(Hbase132ReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持此类模式:" + modeType);
            }

        }

        @Override
        public void destroy() {

        }
    }

}
