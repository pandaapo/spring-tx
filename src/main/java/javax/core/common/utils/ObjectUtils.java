package javax.core.common.utils;

public class ObjectUtils {

    /**
     * 参数1是否为空对象 是则返回参数2
     * @param obj
     * @param obj1
     * @return
     */
    public static Object notNull(Object obj, Object obj1) {
        return (obj == null || "".equals(obj)) ? obj1 : obj;
    }
}
