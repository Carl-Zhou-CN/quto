package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SecStockWindowFunction02 implements WindowFunction<CleanBean, StockBean,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        //获取窗口最新数据
        CleanBean cleanBean = FunctionUtil.newCleanBean(input);
        //格式化时间
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        //封装对象
        StockBean stockBean = new StockBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePx(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                cleanBean.getTradeVolumn(),
                cleanBean.getTradeAmt(),
                0L, 0L,
                tradeTime,
                cleanBean.getSource()
        );
        out.collect(stockBean);
    }
}
