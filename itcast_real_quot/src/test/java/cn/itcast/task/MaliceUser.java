package cn.itcast.task;

import cn.itcast.bean.Message;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MaliceUser {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Message> messages = env.fromCollection(
                Arrays.asList(
                        new Message("1", "TMD", 1558430842000L),//2019-05-21 17:27:22
                        new Message("1", "TMD", 1558430843000L),//2019-05-21 17:27:23
                        new Message("1", "TMD", 1558430845000L),//2019-05-21 17:27:25
                        new Message("1", "TMD", 1558430850000L),//2019-05-21 17:27:30
                        new Message("1", "TMD", 1558430851000L),//2019-05-21 17:27:30
                        new Message("2", "TMD", 1558430851000L),//2019-05-21 17:27:31
                        new Message("1", "TMD", 1558430852000L)//2019-05-21 17:27:32
                )
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Message>(Time.seconds(0L)) {
            @Override
            public long extractTimestamp(Message element) {
                return element.getTimeStamp();
            }
        });
        //定义模式规则
        Pattern<Message, Message> pattern = Pattern.<Message>begin("begin")
                .where(new IterativeCondition<Message>() {
                    @Override
                    public boolean filter(Message message, Context<Message> context) throws Exception {
                        return message.getMsg().equals("TMD");
                    }
                }).times(2)
                .within(Time.seconds(5));//5秒内出现两次TMD
        PatternStream<Message> cep = CEP.pattern(messages.keyBy(Message::getId), pattern);

        SingleOutputStreamOperator<List<Message>> result = cep.select(new PatternSelectFunction<Message, List<Message>>() {
            @Override
            public List<Message> select(Map<String, List<Message>> map) throws Exception {
                List<Message> begin = map.get("begin");
                return begin;
            }
        });

        result.print();

        env.execute();

    }
}
