package com.orm.orm;

import com.orm.transaction.entity.Member;

import javax.persistence.Column;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

public class JdbcTest {

    public static void main(String[] args) {
        Member condition = new Member();
        condition.setName("吴");
        List<?> result = select(condition);
        System.out.println(Arrays.toString(result.toArray()));
    }

    private static List<?> select(Object condition) {
        List<Object> result = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstm = null;
        ResultSet rs = null;
        Class<?> entityClass = condition.getClass();
        try {
            //1、加载驱动类
            Class.forName("com.mysql.jdbc.Driver");
            //2、建立连接
            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/orm-tx?characterEncoding=UTF-8&rewriteBatchedStatements=true&&useJDBCCompliantTimezoneShift=true&serverTimezone=UTC", "root", "123456");

            ///优化思路：（1）通过实体类属性的@Column注解将属性名和数据库表的字段名对应起来。通过反射机制可以拿到实体类的所有字段。（2）sql中的表名通过实体类的@Table注解名获取。（3）给sql自动拼上查询条件。
            Field[] fields = entityClass.getDeclaredFields();
            Map<String, String> mapper = new HashMap<>();
            Map<String, String> getColumnNameByFieldName = new HashMap<>();
            for (Field field : fields) {
                String fieldName = field.getName();
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    mapper.put(column.name(),fieldName);
                    getColumnNameByFieldName.put(fieldName, column.name());
                } else {
                    mapper.put(fieldName,fieldName);
                    getColumnNameByFieldName.put(fieldName, fieldName);
                }
            }

            StringBuffer where = new StringBuffer(" where 1=1 ");
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(condition);
                if (null != value) {
                    if (String.class == field.getType()) {
                        where.append(" and " + getColumnNameByFieldName.get(field.getName()) + "='" +value+ "'");
                    }else{
                        where.append(" and " +getColumnNameByFieldName.get(field.getName()) + "=" +value);
                    }
                }
            }

            //3、创建语句集
            Table table = entityClass.getAnnotation(Table.class);
            String sql = "select * from " + table.name();
            System.out.println(sql + where.toString());
            pstm = connection.prepareStatement(sql + where.toString());
            //4、执行语句集
            rs= pstm.executeQuery();

            ////行记录的元数据。元数据：保存了除了真正数值以外的所有附加信息
            int columnCounts = rs.getMetaData().getColumnCount();
            Object instance = entityClass.newInstance();
            //5、获取结果集
            while (rs.next()) {
                for (int i = 1; i <= columnCounts; i++) {
                    ///从rs中取得当前这个位置的列名
                    String columnName = rs.getMetaData().getColumnName(i);
                    Field field = entityClass.getDeclaredField(mapper.get(columnName));
                    ///可能是私有属性
                    field.setAccessible(true);
                    ///instance这个对象参数可以直接用本方法的入参condition吗？？？
                    field.set(instance, rs.getObject(columnName));
                }
                result.add(instance);
//                Member m = new Member();
//                m.setId(rs.getLong("id"));
//                m.setAge(rs.getInt("age"));
//                m.setAddr(rs.getString("addr"));
//                m.setName(rs.getString("name"));
//                result.add(m);

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

//    private static List<Member> select(String sql) {
//        List<Member> result = new ArrayList<>();
//        Connection con = null;
//        PreparedStatement pstm = null;
//        ResultSet rs = null;
//        try {
//            //1、加载驱动类
//            Class.forName("com.mysql.jdbc.Driver");
//            //2、建立连接
//            con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/gp-vip-spring-db-demo","root","123456");
//            //3、创建语句集
//            pstm =  con.prepareStatement(sql);
//            //4、执行语句集
//            rs = pstm.executeQuery();
//            while (rs.next()){
//                Member instance = new Member();
//                instance.setId(rs.getLong("id"));
//                instance.setName(rs.getString("name"));
//                instance.setAge(rs.getInt("age"));
//                instance.setAddr(rs.getString("addr"));
//                result.add(instance);
//            }
//            //5、获取结果集
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        //6、关闭结果集、关闭语句集、关闭连接
//        finally {
//            try {
//                rs.close();
//                pstm.close();
//                con.close();
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//        return result;
//    }



//    private static List<Member> select(String sql) {
//        List<Member> result = new ArrayList<>();
//        Connection con = null;
//        PreparedStatement pstm = null;
//        ResultSet rs = null;
//        try {
//            //1、加载驱动类
//            Class.forName("com.mysql.jdbc.Driver");
//            //2、建立连接
//            con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/gp-vip-spring-db-demo","root","123456");
//            //3、创建语句集
//            pstm =  con.prepareStatement(sql);
//            //4、执行语句集
//            rs = pstm.executeQuery();
//            while (rs.next()){
//                Member instance = mapperRow(rs,rs.getRow());
//                result.add(instance);
//            }
//            //5、获取结果集
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        //6、关闭结果集、关闭语句集、关闭连接
//        finally {
//            try {
//                rs.close();
//                pstm.close();
//                con.close();
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//        return result;
//    }
//
//    private static Member mapperRow(ResultSet rs, int i) throws Exception {
//        Member instance = new Member();
//        instance.setId(rs.getLong("id"));
//        instance.setName(rs.getString("name"));
//        instance.setAge(rs.getInt("age"));
//        instance.setAddr(rs.getString("addr"));
//        return instance;
//    }
}
