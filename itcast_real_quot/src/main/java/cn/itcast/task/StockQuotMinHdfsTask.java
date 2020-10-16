package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.StockPutHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

public class StockQuotMinHdfsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.设置HDFS存储路径
         * 2.设置数据文件参数
         *   （大小、分区、格式、前缀、后缀）
         * 3.数据分组
         * 4.划分时间窗口
         * 5.数据处理
         * 6.转换并封装数据
         * 7.写入HDFS
         */
        //1.设置HDFS存储路径
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("stock.sec.hdfs.path"));
        //2.设置数据文件参数
        //大小、分区、格式、前缀、后缀）
        bucketingSink.setBucketer(new DateTimeBucketer<>(QuotConfig.config.getProperty("hdfs.bucketer")));
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        bucketingSink.setInProgressPrefix("stock-");
        bucketingSink.setPendingPrefix("stock2-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");
        //3.数据分组
        waterData
                .keyBy(new KeySelectorByCode())
                //4.划分时间窗口
                .timeWindow(Time.seconds(60))
                //5.数据处理
                .apply(new MinStockWindowFunction())
                //6.转换并封装数据
                .map(new StockPutHdfsMap())
                //7.写入HDFS
                .addSink(bucketingSink);




    }
}
