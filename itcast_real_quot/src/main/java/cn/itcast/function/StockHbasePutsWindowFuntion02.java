package cn.itcast.function;

import cn.itcast.bean.StockBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class StockHbasePutsWindowFuntion02 implements AllWindowFunction<StockBean,List<Put>,TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<StockBean> input, Collector<List<Put>> out) throws Exception {
        ArrayList<Put> list = new ArrayList<>();
        for (StockBean stockBean : input) {
            String rowkey=stockBean.getSecCode() + stockBean.getEventTime();
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data"),Bytes.toBytes(JSON.toJSONString(stockBean)));
            list.add(put);
        }
        out.collect(list);
    }
}
