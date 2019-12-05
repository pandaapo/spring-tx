package javax.core.common.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * 扩展Apache Commons BeanUtils, 提供一些反射方面缺失的封装.
 */
public class BeanUtils extends org.apache.commons.beanutils.BeanUtils {

    /**
     * 根据Class以及Class的父类构造setter的Method数组
     * @param cl
     * @return
     */
    public static List<Method> getSetter(Class cl) {
        List<Method> list = new ArrayList<>();
        Method[] methods = cl.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            String methodName = method.getName();
            if (!methodName.startsWith("set")) {
                continue;
            }
            list.add(method);
        }
        while (true) {
            cl = cl.getSuperclass();
            if (cl == Object.class) {
                break;
            }
            list.addAll(getSetter(cl));
        }
        return list;
    }

    /**
     * 根据Class以及Class的父类构造getter的Method数组，包括布尔类型的
     * @param cl
     * @return
     */
    public static List getGetter(Class<?> cl) {
        List<Method> list = new ArrayList<Method>();
        Method[] methods = cl.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            String methodName = method.getName();
            if (!methodName.startsWith("get") && !methodName.startsWith("is")) {
                continue;
            }
            list.add(method);
        }
        while (true) {
            cl = cl.getSuperclass();
            if (cl == Object.class) {
                break;
            }
            list.addAll(getGetter(cl));
        }
        return list;
    }
}
