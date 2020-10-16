package cn.itcast.sink;

import cn.itcast.util.DbUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class SinkMysql  extends RichSinkFunction<Row> {
    /**
     * 开发步骤：
     * 1.创建构造方法，入参：插入sql
     * 2.初始化mysql
     * 获取JDBC连接，开启事务
     * 3.封装预处理对象
     * 执行更新操作
     * 提交事务
     * 4.关流
     */
    private  String sql;
    private Connection conn = null;
    private PreparedStatement pst = null;
    public SinkMysql(String sql){
        this.sql=sql;
    }
    boolean autoCommit;
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DbUtil.getConnByJdbc();
        autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        pst = conn.prepareStatement(sql);
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        int arity = value.getArity();
        for (int i = 0; i < arity; i++) {
            pst.setObject(i+1, value.getField(i));
        }
        pst.executeUpdate();
        conn.commit();
        conn.setAutoCommit(autoCommit);
    }

    @Override
    public void close() throws Exception {
        if (pst != null) {
            pst.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
