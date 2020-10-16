package cn.itcast.job;

import cn.itcast.avro.AvroDeserializeSchema02;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.map.TMap;
import cn.itcast.task.*;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import javax.annotation.Nullable;
import java.util.Properties;

public class StockStream02 {
    public static void main(String[] args) throws Exception {
        //创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点机制
       // env.enableCheckpointing(5000L);
       // env.setStateBackend(new FsStateBackend(""));
        //设置重启策略
       // env.setRestartStrategy();

        env.setParallelism(1);

        //将kafka中Topic中的数据,对其进行时间过滤,字段过滤
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));
        //沪市行情
        FlinkKafkaConsumer011<SseAvro> sseKafka = new FlinkKafkaConsumer011<SseAvro>(
                QuotConfig.config.getProperty("sse.topic"),
                new AvroDeserializeSchema02(QuotConfig.config.getProperty("sse.topic")),
                properties
        );
        //深市行情
        FlinkKafkaConsumer011<SzseAvro> szseKafka = new FlinkKafkaConsumer011<SzseAvro>(
                QuotConfig.config.getProperty("szse.topic"),
                new AvroDeserializeSchema02(QuotConfig.config.getProperty("szse.topic")),
                properties
        );

        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafka);
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafka);
        //对时间和空字段过滤
        SingleOutputStreamOperator<SseAvro> sseFilterData = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro sseAvro) throws Exception {
                return QuotUtil.checkTime(sseAvro) && QuotUtil.checkData(sseAvro);
            }
        });
        SingleOutputStreamOperator<SzseAvro> szseFilterData = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro szseAvro) throws Exception {
                return QuotUtil.checkTime(szseAvro) && QuotUtil.checkData(szseAvro);
            }
        });
        //将数据封装为CleanBean
        //将两个流整合
        SingleOutputStreamOperator<CleanBean> sseMapData = sseFilterData.map(new TMap<SseAvro>());
        SingleOutputStreamOperator<CleanBean> szseMapData = szseFilterData.map(new TMap<SzseAvro>());

        DataStream<CleanBean> unionData = sseMapData.union(szseMapData);
        //对行情类型过滤
        SingleOutputStreamOperator<CleanBean> filterData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean cleanBean) throws Exception {
                return QuotUtil.isStock(cleanBean);
            }
        });
        //添加水位
        SingleOutputStreamOperator<CleanBean> waterData = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CleanBean>() {
            //延时时间
            long delayTime = Long.parseLong(QuotConfig.config.getProperty("delay.time"));
            //当前最大时间时间
            long currentTimestamp = 0L;

            //添加水位
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimestamp - delayTime - 1);
            }

            //提取事件时间
            @Override
            public long extractTimestamp(CleanBean element, long previousElementTimestamp) {
                Long eventTime = element.getEventTime();
                currentTimestamp = Math.max(currentTimestamp, eventTime);
                return eventTime;
            }
        });

        //个股秒级行情
        new StockQuotSecTask02().process(waterData);
        //分时行情
        new StockQuotMinTask02().process(waterData);
        //分时行情备份
        new StockQuotMinHdfsTask02().process(waterData);
        //涨跌幅行情
        new StockIncrTask02().process(waterData);

        env.execute();
    }
}
