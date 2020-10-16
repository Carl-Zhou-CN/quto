package cn.itcast.map;

import cn.itcast.bean.IndexBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.DbUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class IndexKlineMap extends RichMapFunction<IndexBean, Row> {
    /**
     * 一、初始化
     * 1.创建构造方法
     * 入参：kType：K线类型
     * firstTxdate：周期首个交易日
     * 2.获取交易日历表交易日数据
     * 3.获取周期首个交易日和T日
     * 4.获取K线下的汇总表数据（高、低、成交量、金额）
     */

    String kType; //K线类型
    String firstTxDate; //周期内的首个交易日

    public IndexKlineMap(String kType, String firstTxDate) {
        this.kType = kType;
        this.firstTxDate = firstTxDate;
    }

    String firstTradeDate;
    String tradeDate; //当前日期
    Map<String, Map<String, Object>> aggMap = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取交易日历表交易日数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date = CURDATE()";
        Map<String, String> tradeDateMap = DbUtil.queryKv(sql);
        //获取周期首个交易日和T日
        firstTradeDate = tradeDateMap.get(firstTxDate);
        tradeDate = tradeDateMap.get("trade_date");
        //获取K线下的汇总数据
        String sqlAgg = "SELECT index_code ,MAX(high_price) AS high_price,MIN(low_price) AS low_price ,\n" +
                "SUM(trade_vol) AS trade_vol,SUM(trade_amt) AS trade_amt FROM bdp_quot_index_kline_day\n" +
                "WHERE trade_date BETWEEN '" + firstTxDate + "' AND '" + tradeDate + "' GROUP BY 1";
        aggMap = DbUtil.query("index_code", sqlAgg);
    }

    @Override
    public Row map(IndexBean indexBean) throws Exception {
        /**
         * 1.获取指数部分数据（前收、收、开盘、高、低、量、金额）
         * 2.获取T日和周首次交易日时间,转换成long型
         * 3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
         * 4.获取周/月K数据：成交量、成交额、高、低
         * 5.高、低价格比较
         * 6.计算成交量、成交额
         * 7.计算均价
         * 8.封装数据Row
         */
        //1.获取指数部分数据（前收、收、开盘、高、低、量、金额）
        BigDecimal preClosePrice = indexBean.getPreClosePrice();
        BigDecimal closePrice = indexBean.getClosePrice();
        BigDecimal openPrice = indexBean.getOpenPrice();
        BigDecimal highPrice = indexBean.getHighPrice();
        BigDecimal lowPrice = indexBean.getLowPrice();
        Long tradeVolDay = indexBean.getTradeVolDay();
        Long tradeAmtDay = indexBean.getTradeAmtDay();
        //2.获取T日和周首次交易日时间,转换成long型
        Long firstTradeDateTime = DateUtil.stringToLong(firstTradeDate, Constant.format_yyyy_mm_dd);
        Long tradeDateTime = DateUtil.stringToLong(tradeDate, Constant.format_yyyy_mm_dd);
        //3.比较周期首个交易日和当天交易日大小，判断是否是周、月K线
        if (firstTradeDateTime < tradeDateTime && kType.equals(2) || kType.equals(3)) {
            //4.获取周/月K数据：成交量、成交额、高、低
            if (null != aggMap && aggMap.size() > 0) {
                BigDecimal highPriceLast = new BigDecimal(aggMap.get("high_price").toString());
                BigDecimal lowPriceLast = new BigDecimal(aggMap.get("low_price").toString());
                Long tradeAmtLast = Double.valueOf(aggMap.get("trade_amt").toString()).longValue();
                Long tradeVolLast = Double.valueOf(aggMap.get("trade_vol").toString()).longValue();
                // 5.高、低价格比较
                if (highPrice.compareTo(highPriceLast) == -1) {
                    highPrice = highPriceLast;
                }
                if (lowPrice.compareTo(lowPriceLast) == 1) {
                    lowPrice = lowPriceLast;
                }
                //6.计算成交量、成交额
                tradeVolDay += tradeVolLast;
                tradeAmtDay += tradeAmtLast;
            }
        }
        // 7.计算均价
        BigDecimal avgPrice = new BigDecimal(0);
        if (tradeVolDay != 0) {
            avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, RoundingMode.HALF_UP);
        }
        //8.封装数据Row
        Row row = new Row(13);
        row.setField(0,new Timestamp(new Date().getTime()));
        row.setField(1,tradeDate);
        row.setField(2,indexBean.getIndexCode());
        row.setField(3,indexBean.getIndexName());
        row.setField(4, kType);
        row.setField(5,preClosePrice);
        row.setField(6,openPrice);
        row.setField(7,highPrice);
        row.setField(8,lowPrice);
        row.setField(9,closePrice);
        row.setField(10,avgPrice);
        row.setField(11,tradeVolDay);
        row.setField(12,tradeAmtDay);
        return row;
    }
}
