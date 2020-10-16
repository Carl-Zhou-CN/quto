package cn.itcast.map;

import cn.itcast.bean.SectorBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.DbUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class SectorKlineMap extends RichMapFunction<SectorBean, Row> {
    private  String kType;//K线类型
    private  String firstTxDate;//周期内首个工作日

    public SectorKlineMap(String kType, String firstTxDateType) {
        this.kType=kType;
        this.firstTxDate=firstTxDateType;
    }

    String firstTradeDate = null;
    String tradeDate = null;
    Map<String, Map<String, Object>> klineMap = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取交易日历表交易日数据
        String sql = "SELECT * FROM tcc_date WHERE trade_date = CURDATE()";
        Map<String, String> tradeDateMap = DbUtil.queryKv(sql);
        //获取周期首个交易日和T日
        firstTradeDate = tradeDateMap.get(firstTxDate);
        tradeDate = tradeDateMap.get("trade_date");
        //获取K线下的汇总数据
        String sql2 = "SELECT sec_code ,MAX(high_price) AS high_price,MIN(low_price) AS low_price,SUM(trade_vol) AS trade_vol,\n" +
                "SUM(trade_amt) AS trade_amt FROM bdp_quot_stock_kline_day \n" +
                "WHERE trade_date BETWEEN  '" + firstTradeDate + "' AND '" + tradeDate + "'\n" +
                "GROUP BY sec_code \n";
        klineMap = DbUtil.query("sec_code", sql2);
    }


    @Override
    public Row map(SectorBean sectorBean) throws Exception {
        /**
         * 开发步骤：
         * 一、初始化
         * 1.创建构造方法
         * 入参：kType：K线类型
         * firstTxdate：周期首个交易日
         * 2.获取交易日历表交易日数据
         * 3.获取周期首个交易日和T日
         * 4.获取K线下的汇总表数据（高、低、成交量、金额）
         */
        //获取K线下的汇总表数据（高、低、成交量、金额）
        BigDecimal preClosePrice = sectorBean.getPreClosePrice();
        BigDecimal closePrice = sectorBean.getClosePrice();
        BigDecimal openPrice = sectorBean.getOpenPrice();
        BigDecimal highPrice = sectorBean.getHighPrice();
        BigDecimal lowPrice = sectorBean.getLowPrice();
        Long tradeVolDay = sectorBean.getTradeVolDay();
        Long tradeAmtDay = sectorBean.getTradeAmtDay();
        //获取T日和周期首个交易日时间,转化为Long型
        Long firstTradeDateTime = DateUtil.stringToLong(firstTradeDate, Constant.format_yyyy_mm_dd);
        Long tradeDateTime = DateUtil.stringToLong(tradeDate, Constant.format_yyyy_mm_dd);
        //判断是否为日K
        if (firstTradeDateTime < tradeDateTime && (kType.equals(2)) || kType.equals(2)) {
            //获取周/月K数据:成交量.成交额 高 低
            Map<String, Object> map = klineMap.get(sectorBean.getSectorCode());
            if (null != map && map.size() > 0) {
                BigDecimal highPriceLast = new BigDecimal(map.get("high_price").toString());
                BigDecimal lowPriceLast = new BigDecimal(map.get("low_price").toString());
                Long tradeAmtLast = Double.valueOf(map.get("trade_amt").toString()).longValue();
                Long tradeVolLast = Double.valueOf(map.get("trade_vol").toString()).longValue();
                if (highPrice.compareTo(highPriceLast) == -1) {
                    highPrice = highPriceLast;
                }
                if (lowPrice.compareTo(lowPriceLast) == 1) {
                    lowPrice = lowPriceLast;
                }
                tradeAmtDay += tradeAmtLast;
                tradeVolDay += tradeVolLast;
            }
        }
        BigDecimal avgPrice = new BigDecimal(tradeAmtDay).divide(new BigDecimal(tradeVolDay), 2, BigDecimal.ROUND_HALF_UP);
        Row row = new Row(13);
        row.setField(0, new Timestamp(new Date().getTime()));
        row.setField(1, tradeDate);
        row.setField(2, sectorBean.getSectorCode());
        row.setField(3, sectorBean.getSectorName());
        row.setField(4, kType);
        row.setField(5, preClosePrice);
        row.setField(6, openPrice);
        row.setField(7, highPrice);
        row.setField(8, lowPrice);
        row.setField(9, closePrice);
        row.setField(10, avgPrice);
        row.setField(11, tradeVolDay);
        row.setField(12, tradeAmtDay);
        return row;
    }
}
