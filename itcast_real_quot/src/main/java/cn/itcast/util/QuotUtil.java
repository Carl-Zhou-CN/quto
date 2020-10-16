package cn.itcast.util;

import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;

public class QuotUtil {
    /**
     * 事件时间的过滤
     */
    public static boolean checkTime(Object line) {
        Long timestamp;
        if (line instanceof SseAvro) {
            timestamp = ((SseAvro) line).getTimestamp();
        } else {
            timestamp = ((SzseAvro) line).getTimestamp();
        }

        return timestamp > SpecialTimeUtil.openTime && timestamp < SpecialTimeUtil.closeTime;
    }

    /**
     * 数据过滤：指高开低收为null或者是0的数据
     * 校验 开盘价,最高价,最低价,最新价 是否为 0
     */
    public static boolean checkData(Object line) {
        boolean boolData = false;
        if (line instanceof SseAvro) {
            return ((SseAvro) line).getTradePrice() != 0 &&
                    ((SseAvro) line).getHighPrice() != 0 &&
                    ((SseAvro) line).getLowPrice() != 0 &&
                    ((SseAvro) line).getOpenPrice() != 0;
        } else if (line instanceof SzseAvro) {
            return ((SzseAvro) line).getTradePrice() != 0 &&
                    ((SzseAvro) line).getHighPrice() != 0 &&
                    ((SzseAvro) line).getLowPrice() != 0 &&
                    ((SzseAvro) line).getOpenPrice() != 0;
        }
        return boolData;
    }

    public static boolean isStock(CleanBean cleanBean){
        return "MD002".equals(cleanBean.getMdStreamId())
                || "010".equals(cleanBean.getMdStreamId());
    }
    public static boolean isIndex(CleanBean cleanBean){
        return "MD001".equals(cleanBean.getMdStreamId())
                || "900".equals(cleanBean.getMdStreamId());
    }
}
