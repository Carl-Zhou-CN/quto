package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.function.SectorWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.SectorSinkHdfsMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

public class SectorQuotMinHdfsTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.设置HDFS存储路径
         * 2.设置数据文件参数
         *  （大小、分区、格式、前缀、后缀）
         * 3.数据分组
         * 4.划分个股时间窗口
         * 5.个股窗口数据处理
         * 6.划分板块时间窗口
         * 7.板块窗口数据处理
         * 8.转换并封装数据
         * 9.写入HDFS
         */
        BucketingSink<String> bucketingSink = new BucketingSink<>(QuotConfig.config.getProperty("sector.sec.hdfs.path"));
        bucketingSink.setBatchSize(Long.valueOf(QuotConfig.config.getProperty("hdfs.batch")));
        bucketingSink.setInProgressSuffix("sector-");
        bucketingSink.setPendingPrefix("sector2-");
        bucketingSink.setPendingSuffix(".txt");
        bucketingSink.setInProgressSuffix(".txt");
        bucketingSink.setBucketer(new DateTimeBucketer<String>(QuotConfig.config.getProperty("hdfs.bucketer")));

        waterData
                .keyBy(CleanBean :: getSecCode)
                .timeWindow(Time.seconds(60L))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.seconds(60L))
                .apply(new SectorWindowFunction())
                .keyBy(SectorBean :: getSectorCode)
                .map(new SectorSinkHdfsMap())
                .addSink(bucketingSink);

    }
}
