package com.spring.orm;

import com.spring.transaction.entity.Member;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JdbcTest {

    public static void main(String[] args) {
        List<?> result = select();
        System.out.println(Arrays.toString(result.toArray()));
    }

    private static List<?> select() {
        List<Member> result = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstm = null;
        ResultSet rs = null;
        try {
            //1、加载驱动类
            Class.forName("com.mysql.jdbc.Driver");
            //2、建立连接
            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/spring-tx?characterEncoding=UTF-8&rewriteBatchedStatements=true&&useJDBCCompliantTimezoneShift=true&serverTimezone=UTC", "root", "123456");
            //3、创建语句集
            String sql = "select * from t_member";
            pstm = connection.prepareStatement(sql);
            //4、执行语句集
            rs= pstm.executeQuery();
            //5、获取结果集
            while (rs.next()) {
                Member m = new Member();
                m.setId(rs.getLong("id"));
                m.setAge(rs.getInt("age"));
                m.setAddr(rs.getString("addr"));
                m.setName(rs.getString("name"));
                result.add(m);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //6、关闭结果集、语句集和连接
            try {
                rs.close();
                pstm.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
