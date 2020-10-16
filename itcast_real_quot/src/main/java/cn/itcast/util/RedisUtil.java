package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;

public class RedisUtil {
    /**
     * 1.新建获取连接的方法
     * 2.初始化连接池
     * 3.设置连接集群地址
     * 4.获取客户端连接对象
     */
    public static JedisCluster getJedisCluster(){
       //2.初始化连接池
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //端口号
        String host = QuotConfig.config.getProperty("redis.host");
        //最大连接数
        String maxTotal = QuotConfig.config.getProperty("redis.maxTotal");
        //最小空闲连接数
        String minIdle = QuotConfig.config.getProperty("redis.minIdle");
        //获取set集合
        String maxIdle = QuotConfig.config.getProperty("redis.maxIdle");
        HashSet<HostAndPort> set = new HashSet<>();
        String[] split = host.split(",");
        for (String str : split) {
            String[] arr = str.split(":");
            set.add(new HostAndPort(arr[0],Integer.valueOf(arr[1])));
        }
        //设置连接池
        jedisPoolConfig.setMaxIdle(Integer.valueOf(maxIdle));
        jedisPoolConfig.setMinIdle(Integer.valueOf(minIdle));
        jedisPoolConfig.setMaxTotal(Integer.valueOf(maxTotal));

        JedisCluster jedisCluster = new JedisCluster(set, jedisPoolConfig);
        return jedisCluster;
    }

    public static void main(String[] args) throws IOException {
        JedisCluster jedisCluster = getJedisCluster();
        jedisCluster.hset("quot", "zf", "-1") ;//振幅
        jedisCluster.hset("quot", "upDown1", "-1"); //涨跌幅-跌幅
        jedisCluster.hset("quot", "upDown2", "100") ;//涨跌幅-涨幅
        jedisCluster.hset("quot", "hsl", "-1") ;//换手率
        jedisCluster.close();
    }
}
