package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.MinStockWindowFunction02;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.StockPutHdfsMap;
import cn.itcast.map.StockPutHdfsMap02;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

//分时行情备份
public class StockQuotMinHdfsTask02 implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //创建hdfs文件对象
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("stock.sec.hdfs.path"));
        //设置格式
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer")));
        //设置大小
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        //设置前缀,后缀
        bucketingSink.setInProgressPrefix("stock-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingPrefix("stock2-");
        bucketingSink.setPendingSuffix(".txt");

        waterData
                .keyBy(new KeySelectorByCode())
                .timeWindow(Time.seconds(60))
                .apply(new MinStockWindowFunction02())
                .map(new StockPutHdfsMap02())
                .addSink(bucketingSink);
    }
}
