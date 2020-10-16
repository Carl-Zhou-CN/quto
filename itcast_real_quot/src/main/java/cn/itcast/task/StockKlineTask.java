package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.StockKlineMap;
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

public class StockKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.新建侧边流分支（周、月）
         * 2.数据分组
         * 3.划分时间窗口
         * 4.数据处理
         * 5.分流、封装侧边流数据
         * 6.编写插入sql
         * 7.（日、周、月）K线数据写入
         * 数据转换
         * 数据分组
         * 数据写入mysql
         */
        OutputTag<StockBean> weekOutPut = new OutputTag<>("week", TypeInformation.of(StockBean.class));
        OutputTag<StockBean> monthOutPut = new OutputTag<>("month", TypeInformation.of(StockBean.class));

        SingleOutputStreamOperator<StockBean> processData = waterData
                .keyBy(CleanBean::getSecCode)
                .timeWindow(Time.seconds(60L))
                .apply(new MinStockWindowFunction())
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        ctx.output(weekOutPut, value);
                        ctx.output(monthOutPut, value);
                        out.collect(value);
                    }
                });

        //6.编写插入sql
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //数据转化日k
        processData.map(new StockKlineMap(KlineType.DAYK.getType(), KlineType.DAYK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.day.table"))));

        //数据转化周k
        processData.getSideOutput(weekOutPut).map(new StockKlineMap(KlineType.WEEKK.getType(), KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.week.table"))));
        //数据转化月k
        processData.getSideOutput(monthOutPut).map(new StockKlineMap(KlineType.MONTHK.getType(), KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.stock.sql.month.table"))));
    }
}
