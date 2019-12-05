package com.orm.framework;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

/**工具类：1、获取类的一些信息：属性、getter、setter。2、定义了默认支持的类型转换**/
public class ClassMappings {
    private ClassMappings() {
    }

    static final Set<Class<?>> SUPPORTED_SQL_OBJECTS = new HashSet<>();

    static {
        //默认支持的类型自动转换
        Class<?>[] classes = {
                boolean.class, Boolean.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                float.class, Float.class,
                double.class, Double.class,
                String.class,
                Date.class,
                Timestamp.class,
                BigDecimal.class
        };
        SUPPORTED_SQL_OBJECTS.addAll(Arrays.asList(classes));
    }

    static boolean isSupportedSQLObject(Class<?> clazz) {
        return clazz.isEnum() || SUPPORTED_SQL_OBJECTS.contains(clazz);
    }

    //构建Map：key是类中的getter方法名"get"以后的部分（布尔类型也有处理），value是对应的方法
    public static Map<String, Method> findPublicGetters(Class<?> clazz) {
        Map<String, Method> map = new HashMap<>();
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (Modifier.isStatic(method.getModifiers()))
                continue;
            if (method.getParameterTypes().length != 0)
                continue;
            if (method.getName().equals("getClass"))
                continue;
            Class<?> returnType = method.getReturnType();
            if (void.class.equals(returnType))
                continue;
            if (!isSupportedSQLObject(returnType))
                continue;
            if ((returnType.equals(boolean.class) || returnType.equals(Boolean.class)) && method.getName().startsWith("is") && method.getName().length() > 2) {
                map.put(getGetterName(method), method);
                continue;
            }
            if (!method.getName().startsWith("get"))
                continue;
            if (method.getName().length() < 4)
                continue;
            map.put(getGetterName(method), method);
        }
        return map;
    }

    //截取getter方法名
    public static String getGetterName(Method getter) {
        String name = getter.getName();
        if (name.startsWith("is"))
            name = name.substring(2);
        else
            name = name.substring(3);
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }

    public static Field[] findFields(Class<?> clazz) {
        return clazz.getDeclaredFields();
    }

    //构建Map：key是类中的setter方法名"set"以后的部分，value是对应的方法
    public static Map<String, Method> findPublicSetters(Class<?> clazz){
        Map<String, Method> map = new HashMap<>();
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (Modifier.isStatic(method.getModifiers()))
                continue;
            if (!void.class.equals(method.getReturnType()))
                continue;
            if (method.getParameterTypes().length != 1)
                continue;
            if (!method.getName().startsWith("set"))
                continue;
            if (method.getName().length() < 4)
                continue;
            if (!isSupportedSQLObject(method.getParameterTypes()[0]))
                continue;
            map.put(getSetterName(method), method);
        }
        return map;
    }

    //截取setter方法名
    private static String getSetterName(Method setter) {
        String name = setter.getName().substring(3);
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
}
