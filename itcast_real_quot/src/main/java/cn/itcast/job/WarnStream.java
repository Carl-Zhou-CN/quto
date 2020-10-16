package cn.itcast.job;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.map.TMap;
import cn.itcast.task.AmplitudeTask;
import cn.itcast.task.StockUpdownTask;
import cn.itcast.task.TurnoverRateTask;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
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

public class WarnStream {
    public static void main(String[] args) throws Exception {
        /**
         *   1.创建WarnStream对象，创建main方法
         *       2.获取流处理执行环境
         *       3.设置事件时间、并行度
         *       4.设置检查点机制
         *       5.设置重启机制
         *       6.整合Kafka
         *       7.数据过滤（时间和null字段）
         *       8.数据转换、合并
         *       9.过滤个股数据
         *       10.设置水位线
         * 11.业务处理（振幅、涨跌幅、换手率）
         *       12.触发执行
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));

        FlinkKafkaConsumer011<SseAvro> sseKafka = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.topic"), new AvroDeserializeSchema(QuotConfig.config.getProperty("sse.topic")), properties);
        sseKafka.setStartFromEarliest();
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafka);

        //sseSource.print("沪市 :");

        FlinkKafkaConsumer011<SzseAvro> szseKafka = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.config.getProperty("szse.topic"), new AvroDeserializeSchema(QuotConfig.config.getProperty("szse.topic")), properties);
        szseKafka.setStartFromEarliest();
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafka);

        //szseSource.print("深市:");

        SingleOutputStreamOperator<SseAvro> sseFilterData  = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro sseAvro) throws Exception {
                return QuotUtil.checkTime(sseAvro) && QuotUtil.checkData(sseAvro);
            }
        });

        SingleOutputStreamOperator<SzseAvro> szseFilterData  = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro szseAvro) throws Exception {
                return QuotUtil.checkTime(szseAvro) && QuotUtil.checkData(szseAvro);
            }
        });

        SingleOutputStreamOperator<CleanBean> sseMapData = sseFilterData.map(new TMap<SseAvro>());
        SingleOutputStreamOperator<CleanBean> szseMapData = szseFilterData.map(new TMap<SzseAvro>());

        DataStream<CleanBean> unionData = sseMapData.union(szseMapData);

        SingleOutputStreamOperator<CleanBean> filterData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean cleanBean) throws Exception {
                return QuotUtil.isStock(cleanBean);
            }
        });
        SingleOutputStreamOperator<CleanBean> waterData = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CleanBean>() {
            //延时时间
            long delayTime = Long.parseLong(QuotConfig.config.getProperty("delay.time"));
            //当前最大时间时间
            long currentTimestamp = 0L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimestamp - delayTime - 1);
            }

            @Override
            public long extractTimestamp(CleanBean cleanBean, long l) {
                currentTimestamp = Math.max(currentTimestamp, cleanBean.getEventTime());
                return cleanBean.getEventTime();
            }
        });

        //振幅
        new AmplitudeTask().process(waterData,env);
        //涨跌幅
        new StockUpdownTask().process(waterData,env);
        //换手率
        new TurnoverRateTask().process(waterData,env);
        env.execute();
    }
}
