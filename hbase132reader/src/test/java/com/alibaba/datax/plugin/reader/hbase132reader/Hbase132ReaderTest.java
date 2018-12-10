package com.alibaba.datax.plugin.reader.hbase132reader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author jack_yang
 * @date 2018/12/9
 */
public class Hbase132ReaderTest {

    private String jsonStr = "{\n" +
            "\t\"hbaseConfig\": {\n" +
            "\t\t\"hbase.zookeeper.quorum\": \"localhost\"\n" +
            "\t},\n" +
            "\t\"table\": \"T5\",\n" +
            "\t\"column\": []\n" +
            "}";

    private List<Configuration> generateSplitConfig() throws IOException, InterruptedException {
        Configuration config = Configuration.from(jsonStr);

        return  Hbase132Helper.split(config);

    }

    @Test
    public void testReadRecord() throws Exception {
        List<Configuration> splits = this.generateSplitConfig();
        if (splits != null) {
            int allRecordNum = 0;
            for (int i = 0; i < splits.size(); i++) {
                RecordSender recordSender = mock(RecordSender.class);
                when(recordSender.createRecord()).thenReturn(new DefaultRecord());
                Record record = recordSender.createRecord();
                Hbase132Reader.Task hbaseReaderTask = new Hbase132Reader.Task();
                hbaseReaderTask.init();
                hbaseReaderTask.prepare();

                int num = 0;
                while (true) {
                    boolean hasLine = false;
                    try {
                        hbaseReaderTask.startRead(recordSender);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw e;
                    }
                    if (!hasLine)
                        break;
                    num++;
                    if (num % 100 == 0)
                        System.out.println("record num is :" + num + ",record is " + record.toString());
                    when(recordSender.createRecord()).thenReturn(new DefaultRecord());
                    String recordStr = "";
                    for (int j = 0; j < record.getColumnNumber(); j++) {
                        recordStr += record.getColumn(j).asString() + ",";
                    }
                    recordSender.sendToWriter(record);
                    record = recordSender.createRecord();
                }

                System.out.println("all record num = " + allRecordNum);
                assertEquals(0, allRecordNum);
            }
        }

    }
}
