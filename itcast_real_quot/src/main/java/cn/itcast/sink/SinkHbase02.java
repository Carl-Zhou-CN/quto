package cn.itcast.sink;

import cn.itcast.util.HbaseUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;


import java.util.List;

public class SinkHbase02 implements SinkFunction<List<Put>> {

    private String tableName;

    public SinkHbase02(String tableName){
        this.tableName=tableName;
    }

    @Override
    public void invoke(List<Put> value, Context context) throws Exception {
        HbaseUtil.putList(tableName, value);
    }
}
