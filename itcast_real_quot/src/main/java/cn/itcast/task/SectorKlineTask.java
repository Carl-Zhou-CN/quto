package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.KlineType;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.function.SectorWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.map.SectorKlineMap;
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

public class SectorKlineTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.数据分组
         * 2.划分时间窗口
         * 3.数据处理
         * 4.编写插入sql
         * 5.（日、周、月）K线数据写入
         * 数据转换、分组
         * 数据写入mysql
         */
        OutputTag<SectorBean> weekOutPut = new OutputTag<SectorBean>("week", TypeInformation.of(SectorBean.class));
        OutputTag<SectorBean> monthOutPut = new OutputTag<SectorBean>("month", TypeInformation.of(SectorBean.class));

        SingleOutputStreamOperator<SectorBean> processData = waterData
                .keyBy(CleanBean::getSecCode)
                .timeWindow(Time.seconds(60L))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.seconds(60L))
                .apply(new SectorWindowFunction())
                .process(new ProcessFunction<SectorBean, SectorBean>() {
                    @Override
                    public void processElement(SectorBean value, Context ctx, Collector<SectorBean> out) throws Exception {
                        ctx.output(weekOutPut, value);
                        ctx.output(monthOutPut, value);
                        out.collect(value);
                    }
                });
        String sql = "replace into %s values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        processData.map(new SectorKlineMap(KlineType.DAYK.getType(),KlineType.DAYK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.day.table"))))
                .setParallelism(2);
        processData.map(new SectorKlineMap(KlineType.WEEKK.getType(),KlineType.WEEKK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.week.table"))))
                .setParallelism(2);
        processData.map(new SectorKlineMap(KlineType.MONTHK.getType(),KlineType.MONTHK.getFirstTxDateType()))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return row.getField(2).toString();
                    }
                }).addSink(new SinkMysql(String.format(sql, QuotConfig.config.getProperty("mysql.sector.sql.month.table"))))
                .setParallelism(2);

    }
}
