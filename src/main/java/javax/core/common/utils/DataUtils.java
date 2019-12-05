package javax.core.common.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DataUtils {

    private static Log logger = LogFactory.getLog(DataUtils.class);

    /**mergePO时支持的数据类型**/
    private static Map<Class, String> supportTypeMap = new HashMap<>();
    static {
        //基本数据类型
        supportTypeMap.put(Integer.class, "");
        supportTypeMap.put(Long.class, "");
        supportTypeMap.put(Double.class, "");
        supportTypeMap.put(Byte.class, "");
        supportTypeMap.put(Character.class, "");
        supportTypeMap.put(Short.class, "");
        supportTypeMap.put(Float.class, "");
        supportTypeMap.put(Boolean.class, "");
        supportTypeMap.put(int.class, "");
        supportTypeMap.put(long.class, "");
        supportTypeMap.put(double.class, "");
        supportTypeMap.put(byte[].class, "");
        supportTypeMap.put(char.class, "");
        supportTypeMap.put(short.class, "");
        supportTypeMap.put(float.class, "");
        supportTypeMap.put(boolean.class, "");
    }

    /**
     * 拷贝简单对象：将源对象的属性值拷贝到目标对象中属性中
     * @param source 源对象
     * @param target 目标对象
     * @param isCopyNull 是否拷贝Null值
     */
    public static void copySimpleObject(Object source, Object target, boolean isCopyNull) {
        if (target == null || source == null) {
            return;
        }
        List targetMethodList = BeanUtils.getSetter(target.getClass());
        List sourceMethodList = BeanUtils.getGetter(source.getClass());
        Map<String, Method> map= new HashMap<>();
        for (Iterator iter = sourceMethodList.iterator(); iter.hasNext();) {
            Method method = (Method) iter.next();
            map.put(method.getName(), method);
        }
        for (Iterator iter = targetMethodList.iterator(); iter.hasNext();) {
            Method method = (Method) iter.next();
            String fileName = method.getName().substring(3);
            try {
                Method sourceMethod = map.get("get" + fileName);
                if (sourceMethod == null) {
                    sourceMethod = map.get("is" + fileName);
                }
                if (sourceMethod == null) {
                    continue;
                }
                if (!supportTypeMap.containsKey(sourceMethod.getReturnType())) {
                    continue;
                }
                //？？？new Object[0]什么作用？？？
                Object value = sourceMethod.invoke(source, new Object[0]);
                if (isCopyNull) {
                    method.invoke(target, new Object[] { value });
                } else {
                    if (value != null) {
                        method.invoke(target, new Object[] { value });
                    }
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(e);
                }
            }
        }
    }
}
