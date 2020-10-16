package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.KeySelectorByCode02;
import cn.itcast.function.SecStockWindowFunction02;
import cn.itcast.function.StockHbasePutsWindowFuntion02;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase02;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

//个股秒级行情
public class StockQuotSecTask02 implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //分组
        //划分时间窗口
        //处理数据封装成Bean
        //重新划分时间窗口
        //封装成List<Put>
        //写入Hbase
        waterData
                .keyBy(new KeySelectorByCode02())
                .timeWindow(Time.seconds(5))
                .apply(new SecStockWindowFunction02())
                .timeWindowAll(Time.seconds(5))
                .apply(new StockHbasePutsWindowFuntion02())
                .addSink(new SinkHbase02(QuotConfig.config.getProperty("stock.hbase.table.name")));
    }
}
