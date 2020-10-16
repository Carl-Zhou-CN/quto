package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectorByCode implements KeySelector<CleanBean,String>{
    @Override
    public String getKey(CleanBean cleanBean) throws Exception {
        return cleanBean.getSecCode();
    }
}
