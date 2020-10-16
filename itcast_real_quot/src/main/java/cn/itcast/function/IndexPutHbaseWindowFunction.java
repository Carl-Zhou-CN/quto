package cn.itcast.function;

import cn.itcast.bean.IndexBean;
import cn.itcast.util.FunctionUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class IndexPutHbaseWindowFunction implements AllWindowFunction<IndexBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<IndexBean> input, Collector<List<Put>> collector) throws Exception {
        ArrayList<Put> list = new ArrayList<>();
        for (IndexBean indexBean : input) {
            String rowkey=indexBean.getIndexCode()+indexBean.getEventTime();
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data"), Bytes.toBytes(JSON.toJSONString(indexBean)));
            list.add(put);
        }
        collector.collect(list);
    }
}
