package cn.itcast.task;

import cn.itcast.bean.OrderEvent;
import cn.itcast.bean.OrderResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OrderTimeoutDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1.获取流处理执行环境
         * 2.设置并行度,设置事件时间
         * 3.加载数据源,提取事件时间
         * 4.定义匹配模式followedBy，设置时间长度
         * 5.匹配模式（分组）
         * 6.设置侧输出流
         * 7.数据处理(获取begin数据)
         * 8.打印（正常/超时订单）
         * 9.触发执行
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<OrderEvent> orderData = env.fromCollection(
                Arrays.asList(
                        new OrderEvent(1, "create", 1558430842000L),//2019-05-21 17:27:22
                        new OrderEvent(2, "create", 1558430843000L),//2019-05-21 17:27:23
                        new OrderEvent(2, "other", 1558430845000L), //2019-05-21 17:27:25
                        new OrderEvent(2, "pay", 1558430850000L),   //2019-05-21 17:27:30
                        new OrderEvent(1, "pay", 1558431920000L)    //2019-05-21 17:45:20
                )
        );
        SingleOutputStreamOperator<OrderEvent> waterData = orderData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getEventTime();
            }
        });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("begin")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getStatus().equals("create");
                    }
                })
                .followedBy("followed")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getStatus().equals("pay");
                    }
                })
                .within(Time.minutes(15L));

        PatternStream<OrderEvent> cep = CEP.pattern(waterData.keyBy(OrderEvent :: getOrderId), pattern);

        OutputTag<OrderResult> output = new OutputTag<>("output", TypeInformation.of(OrderResult.class));

        SingleOutputStreamOperator<OrderResult> result = cep.select(output, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult timeout(Map<String, List<OrderEvent>> map, long timeoutTimestamp) throws Exception {
                Integer orderId = map.get("begin").get(0).getOrderId();
                return new OrderResult(orderId, "超时订单数据");
            }
        }, new PatternSelectFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
                Integer orderId = map.get("begin").get(0).getOrderId();
                return new OrderResult(orderId, "正常订单数据");
            }
        });

        result.print();
        result.getSideOutput(output).print();

        env.execute();
    }
}
