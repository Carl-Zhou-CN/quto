package cn.itcast.function;

import cn.itcast.bean.SectorBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;


import java.util.ArrayList;
import java.util.List;

public class SectorHbasePutsWindowFunction extends RichAllWindowFunction<SectorBean, List<Put>, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<SectorBean> values, Collector<List<Put>> out) throws Exception {
        ArrayList<Put> list = new ArrayList<>();

        for (SectorBean value : values) {
            String rowkey = value.getSectorCode() + value.getTradeTime();
            String jsonString = JSON.toJSONString(value);
            Put put = new Put(rowkey.getBytes());
            put.addColumn("info".getBytes(), "data".getBytes(), jsonString.getBytes());
            list.add(put);
        }
        out.collect(list);
    }
}
