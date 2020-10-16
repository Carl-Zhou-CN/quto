package cn.itcast.function;

import cn.itcast.bean.StockBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class StockHbasePutsWindowFuntion implements AllWindowFunction<StockBean, List<Put>, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<StockBean> input, Collector<List<Put>> collector) throws Exception {
        /**
         * 1.新建List<Put>
         * 2.循环数据
         * 3.设置rowkey
         * 4.json数据转换
         * 5.封装put
         * 6.收集数据
         */

        ArrayList<Put> list = new ArrayList<>();
        for (StockBean stockBean : input) {
            String rowkey = stockBean.getSecCode()+stockBean.getTradeTime();
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("data"), Bytes.toBytes(JSON.toJSONString(stockBean)));
            list.add(put);
        }
        collector.collect(list);

    }
}
