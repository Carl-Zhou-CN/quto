package cn.itcast.sink;

import cn.itcast.util.HbaseUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

public class SinkHbase implements SinkFunction<List<Put>> {

    private String tableName;

    public SinkHbase(String tableName){
        this.tableName=tableName;
    }

    @Override
    public void invoke(List<Put> list, Context context) throws Exception {
        //HbaseUtil.putList(tableName, list);

        List<Put> puts = new ArrayList<>();
        for (Put put : list) {
            puts.add(put);
        }
        //执行数据写入操作
        HbaseUtil.putList(tableName, puts);
    }

}
