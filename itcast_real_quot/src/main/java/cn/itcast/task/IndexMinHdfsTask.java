package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.MinIndexWindowFunction;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.IndexPutHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;

//分时行情数据备份
public class IndexMinHdfsTask implements ProcessDataInterface {
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
        //3.记录最新指数
        BucketingSink bucketingSink = new BucketingSink(QuotConfig.config.getProperty("index.sec.hdfs.path"));
        //2.设置数据文件参数
        // 大小、分区、格式、前缀、后缀）
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch ")));
        bucketingSink.setBucketer(new DateTimeBucketer(QuotConfig.config.getProperty("hdfs.bucketer ")));
        bucketingSink.setInProgressPrefix("index-");
        bucketingSink.setPendingPrefix("index2-");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setPendingSuffix(".txt");

        waterData
                //3.数据分组
                .keyBy(new KeySelectorByCode())
                //4.划分时间窗口
                .timeWindow(Time.seconds(60))
                //5.数据处理
                .apply(new MinIndexWindowFunction())
                //6.转换并封装数据
                .map(new IndexPutHdfsMap())
                .addSink(bucketingSink);

    }
}
