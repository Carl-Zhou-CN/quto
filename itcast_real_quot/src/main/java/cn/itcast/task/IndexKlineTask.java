package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.MinIndexWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.IndexKlineMap;
import cn.itcast.sink.SinkMysql;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class IndexKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.数据处理
         * 4.编写插入sql
         * 5.（日、周、月）K线数据写入
         * 数据转换、分组
         * 数据写入mysql
         */
        //1.新建侧边流分支（周、月）
        OutputTag<IndexBean> weekOutPut = new OutputTag<IndexBean>("week", TypeInformation.of(IndexBean.class));
        OutputTag<IndexBean> monthOutPut = new OutputTag<IndexBean>("month", TypeInformation.of(IndexBean.class));
        SingleOutputStreamOperator<IndexBean> processData = waterData
                .keyBy(CleanBean::getSecCode)
                .timeWindow(Time.seconds(60L))
                .apply(new MinIndexWindowFunction())
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean value, Context ctx, Collector<IndexBean> out) throws Exception {
                        ctx.output(weekOutPut, value);
                        ctx.output(monthOutPut, value);
                        out.collect(value);
                    }
                });
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //指数日K
        processData
                .map(new IndexKlineMap(KlineType.DAYK.getType(),KlineType.DAYK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(QuotConfig.config.getProperty("mysql.index.sql.day.table")));
        //指数周K
        processData
                .getSideOutput(weekOutPut)
                .map(new IndexKlineMap(KlineType.WEEKK.getType(),KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(QuotConfig.config.getProperty("mysql.index.sql.week.table")));
        //指数月K
        processData
                .getSideOutput(monthOutPut)
                .map(new IndexKlineMap(KlineType.MONTHK.getType(),KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(QuotConfig.config.getProperty("mysql.index.sql.month.table")));

    }
}
