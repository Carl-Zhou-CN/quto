package cn.itcast.job;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.map.TMap;
import cn.itcast.task.IndexKlineTask;
import cn.itcast.task.IndexMinHdfsTask;
import cn.itcast.task.IndexMinTask;
import cn.itcast.task.IndexSecTask;
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

public class IndexStream {
    public static void main(String[] args) throws Exception {
        /**
         * 1.创建StockStream单例对象，创建main方法
         *   2.获取流处理执行环境
         *   3.设置事件时间
         *   4.设置检查点机制
         *   5.设置重启机制
         * 6.触发执行
          */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

     /*   env.enableCheckpointing(5000L);
        env.setStateBackend(new FsStateBackend(QuotConfig.config.getProperty("stock.sec.hdfs.path")));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);;
        env.getCheckpointConfig().setCheckpointTimeout(60000L);;
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);;

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));;
*/

        /**
         * 1.创建Properties对象
         * 2. 新建消费对象
         * 3.新建反序列化对象
         * 4. 设置消费模式
         * 5. 添加source获取DataStream
         * 6. 打印DataStream
         */

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));

        FlinkKafkaConsumer011<SseAvro> sseKafka = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.stock.topic"), new AvroDeserializeSchema(QuotConfig.config.getProperty("sse.stock.topic")), properties);
       sseKafka.setStartFromEarliest();
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafka);

        //sseSource.print("沪市 :");


        FlinkKafkaConsumer011<SzseAvro> szseKafka = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.config.getProperty("szse.stock.topic"), new AvroDeserializeSchema(QuotConfig.config.getProperty("szse.stock.topic")), properties);
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
                return QuotUtil.isIndex(cleanBean);
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

        //指数秒级行情处理
        new IndexSecTask().process(waterData);
        //分时行情数据
        new IndexMinTask().process(waterData);
        //分时行情数据备份
        new IndexMinHdfsTask().process(waterData);
        //指数K线
        new IndexKlineTask().process(waterData);

        env.execute();
    }
}
