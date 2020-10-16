package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncrBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class StockMinIncrWindowFunction implements WindowFunction<CleanBean,StockIncrBean,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> input, Collector<StockIncrBean> collector) throws Exception {
        /**
         * 开发步骤：
         * 1.新建MinStockIncrWindowFunction 窗口函数
         * 2.记录最新个股
         * 3.格式化日期
         * 4.指标计算
         *   涨跌、涨跌幅、振幅
         * 5.封装输出数据
         */
        //2.记录最新个股
        CleanBean cleanBean = FunctionUtil.newCleanBean(input);
        //3.格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        //4.指标计算
        //涨跌、涨跌幅、振幅
        //今日涨跌=当前价-前收盘价
        //今日涨跌幅（%）=（当前价-前收盘价）/ 前收盘价 * 100%
        //今日振幅 =（当日最高点的价格－当日最低点的价格）/昨天收盘价×100%
        BigDecimal upDown = cleanBean.getTradePrice().subtract(cleanBean.getPreClosePx());
        BigDecimal increase = upDown.divide(cleanBean.getPreClosePx(), 2, RoundingMode.HALF_UP);
        BigDecimal amplitude = (cleanBean.getMaxPrice().subtract(cleanBean.getMinPrice())).divide(cleanBean.getPreClosePx(),2,RoundingMode.HALF_UP);
        StockIncrBean stockIncrBean = new StockIncrBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                increase,
                cleanBean.getTradePrice(),
                upDown,
                cleanBean.getTradeVolumn(),
                amplitude,
                cleanBean.getPreClosePx(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );

        collector.collect(stockIncrBean);

    }
}
