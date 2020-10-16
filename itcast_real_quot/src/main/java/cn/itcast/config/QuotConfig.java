package cn.itcast.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;


public class QuotConfig {

    public static  final Logger LOG= LoggerFactory.getLogger(QuotConfig.class);
    public final static Properties config = new Properties();
    static {
        InputStream profile = QuotConfig.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            config.load(profile);
        } catch (IOException e) {
            LOG.info("load profile error!");
            e.printStackTrace();
        }
        for (Map.Entry<Object, Object> kv : config.entrySet()) {
            LOG.info(kv.getKey()+"="+kv.getValue());
        }
    }
}
