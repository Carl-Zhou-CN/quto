package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HbaseUtil {
    /**
     * 1.静态代码块获取连接对象
     * 2.获取表
     * 3.插入单列数据
     * 4.插入多列数据
     * 5.根据rowkey查询数据
     * 6.根据rowkey删除数据
     * 7.批量数据插入
     */
    private static final Logger log = LoggerFactory.getLogger(HbaseUtil.class);
    static  Connection connection=null;
    static {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", QuotConfig.config.getProperty("zookeeper.connect"));
        try {
             connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("hbase连接异常", e);
        }
    }
    //2.获取表
    public static Table getTable(String tableName){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error("hbase->table获取失败", e);
        }
        return table;
    }

    //3.插入单列数据
    public static void putDataByRowkey(String tableName,String rowkey,String family, String colName, String colVal){
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        try {
            table.put(put);
        } catch (IOException e) {
            log.error("插入表数据失败", e);
        }finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //4.插入多列数据
    public static void putMapDataByRowkey(String tableName, String rowkey, String family, Map<String,Object> map){
        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));

        for (String key : map.keySet()) {
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(key), Bytes.toBytes(map.get(key).toString()));
        }
        try {
            table.put(put);
        } catch (IOException e) {
            log.error("插入表数据失败", e);
        }finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //5.根据rowkey查询数据
    public static String  queryByRowkey(String tableName, String rowkey, String family, String colName){
        Table table = getTable(tableName);
        String str= null;
        try {
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            byte[] bytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes(colName));
            str = null;
            if (bytes !=null){
                str = new String(bytes);
            }
        } catch (IOException e) {
            log.error("获取表数据失败", e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return str;
    }
    //6.根据rowkey删除数据
    public static void delByRowkey(String tableName,String rowkey,String familey){
        Table table = getTable(tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addFamily(Bytes.toBytes(familey) );
            table.delete(delete);
        } catch (IOException e) {
            log.error("删除数据失败", e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //7.批量数据插入
    public static void putList(String tableName, List<Put> puts){
        Table table = getTable(tableName);
        try {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //区间查询
    public static  List<String> scanQuery(String tableName, String family, String colName, String startKey, String endKey){
        Table table = getTable(tableName);
        List<String> list = null;
        try {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStartRow(Bytes.toBytes(endKey));
            ResultScanner resultScanner = table.getScanner(scan);
            list = new ArrayList<>();
            for (Result result : resultScanner) {
                String str = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(colName)));
                list.add(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

}
