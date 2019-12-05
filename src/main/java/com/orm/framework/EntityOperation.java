package com.orm.framework;

import javafx.scene.control.Tab;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;

import javax.persistence.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 实体对象的反射操作
 */
public class EntityOperation<T> {
    private Logger log = Logger.getLogger(EntityOperation.class);
    public Class<T> entityClass = null; //泛型实体Class对象
    public final Map<String, PropertyMapping> mappings; //Map》字段/属性名称：字段/属性信息
    public final RowMapper<T> rowMapper; //实现RowMapper
    public final String tableName; //数据库表名
    public String allColumn = "*";
    public Field pkField; //主键字段/属性

    //初始化以上各属性
    public EntityOperation(Class<T> clazz, String pk)  throws Exception{
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new Exception("在" + clazz.getName() + "中没有找到Entity注解，不能做ORM映射");
        }
        this.entityClass = clazz;
        Table table = entityClass.getAnnotation(Table.class);
        if (table != null) {
            this.tableName = table.name();
        } else {
            this.tableName = entityClass.getSimpleName();
        }
        Map<String, Method> getters = ClassMappings.findPublicGetters(entityClass);
        Map<String, Method> setters = ClassMappings.findPublicSetters(entityClass);
        Field[] fields = ClassMappings.findFields(entityClass);
        fillPKFieldAndAllColumn(pk,fields);
        this.mappings = getPropertyMappings(getters, setters, fields);
        this.allColumn = this.mappings.keySet().toString().replace("[","").replace("]","").replaceAll(" ","");
        this.rowMapper = createRowMapper();
    }

    //实现RowMapper：将ResultSet对应的行记录，设置为Entity类的对应字段/属性值。
    private RowMapper<T> createRowMapper() {
        return new RowMapper<T>() {
            @Override
            public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                try {
                    T t = entityClass.newInstance();
                    ResultSetMetaData meta = rs.getMetaData();
                    int columns = meta.getColumnCount();
                    String columnName;
                    for (int i = 0; i < columns; i++) {
                        Object value = rs.getObject(i);
                        columnName = meta.getColumnName(i);
                        fillBeanFieldValue(t, columnName, value);
                    }
                    return t;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        };
    }

    //给字段/属性设值
    protected void fillBeanFieldValue(T t, String columnName, Object value) {
        if (value != null) {
            PropertyMapping pm = mappings.get(columnName);
            if (pm != null) {
                try {
                    pm.set(t, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //构建Map：key字段/属性名，value：字段的一些信息
    private Map<String, PropertyMapping> getPropertyMappings(Map<String, Method> getters, Map<String, Method> setters, Field[] fields) {
        Map<String, PropertyMapping> mappings = new HashMap<>();
        String name;
        for (Field field : fields) {
            if (field.isAnnotationPresent(Transient.class))
                continue;
            name = field.getName();
            if (name.startsWith("is")) {
                name = name.substring(2);
            }
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            Method setter = setters.get(name);
            Method getter = getters.get(name);
            if (setter == null || getter == null) {
                continue;
            }
            Column column = field.getAnnotation(Column.class);
            if (column == null) {
                mappings.put(field.getName(), new PropertyMapping(getter, setter, field));
            } else {
                mappings.put(column.name(), new PropertyMapping(getter, setter, field));
            }
        }
        return mappings;
    }

    //获得对应主键列的字段/属性
    private void fillPKFieldAndAllColumn(String pk, Field[] fields) {
        //设定主键
        try {
            if (!StringUtils.isEmpty(pk)){
                pkField = entityClass.getDeclaredField(pk);
                pkField.setAccessible(true);
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            log.debug("没找到主键列，主键列名必须与属性名相同");
        }
        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            if (StringUtils.isEmpty(pk)) {
                Id id = f.getAnnotation(Id.class);
                if (id != null) {
                    pkField = f;
                    break;
                }
            }
        }
    }

    //构建Map》字段/属性名称：getter()的返回值
    public Map<String, Object> parse(T t) {
        Map<String, Object> _map = new TreeMap<>();
        try {
            for (String columnName : mappings.keySet()) {
                Object value = mappings.get(columnName).getter.invoke(t);
                if (value == null)
                    continue;
                _map.put(columnName, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return _map;
    }
}

/**entity字段的各种信息：是否允许执行插入、是否允许执行更新、对应数据库列名、是不是主键id，对应的getter和setter、字段名/属性名。 ？？？enumClass？？？**/
class PropertyMapping {
    final boolean inserttable;
    final boolean updatatable;
    final String columnName;
    final boolean id;
    final Method getter;
    final Method setter;
    final Class enumClass;
    final String fieldName;

    public PropertyMapping(Method getter, Method setter, Field field) {
        this.getter = getter;
        this.setter = setter;
        this.enumClass = getter.getReturnType().isEnum() ? getter.getReturnType() : null;
        Column column = field.getAnnotation(Column.class);
        this.inserttable = column == null || column.insertable();
        this.updatatable = column == null || column.updatable();
        this.columnName = column == null ? ClassMappings.getGetterName(getter) : ("".equals(column.name()) ? ClassMappings.getGetterName(getter) : column.name());
        this.id = field.isAnnotationPresent(Id.class);
        this.fieldName = field.getName();
    }

    //？？？什么意思？？？
    Object get(Object target) throws Exception {
        Object r = getter.invoke(target);
        return enumClass == null ? r : Enum.valueOf(enumClass, (String) r);
    }

    void set(Object target, Object value) throws Exception {
        if (enumClass != null && value != null) {
            value = Enum.valueOf(enumClass, (String) value);
        }
        try {
            if (value != null) {
                setter.invoke(target, setter.getParameterTypes()[0].cast(value));
            }
        } catch (Exception e) {
            e.printStackTrace();
            /**
             * 出错原因如果是boolean字段 mysql字段类型 设置tinyint(1)
             */
            System.err.println(fieldName + "--" + value);
        }
    }
}
