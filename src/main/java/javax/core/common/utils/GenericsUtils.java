package javax.core.common.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * 泛型操作类
 */
public class GenericsUtils {
    private static final Log logger = LogFactory.getLog(GenericsUtils.class);

    private GenericsUtils() {
    }

    public static Class getSuperClassGenricType(Class clazz) {
        return getSuperClassGenricType(clazz, 0);
    }

    /**
     * 通过反射，获得定义Class时声明的父类的泛型参数的类型，如public BookManager extends GenricManager<Book>中的Book
     * @param clazz
     * @param index
     * @return
     */
    public static Class getSuperClassGenricType(Class clazz, int index) {
        Type genType = clazz.getGenericSuperclass();
        if (!(genType instanceof ParameterizedType)) {
            logger.debug(clazz.getSimpleName()+ "'s superclass not ParammeterizedType");
            return Object.class;
        }
        //获得父/超类的泛型参数的实际类型
        Type[] params = ((ParameterizedType)genType).getActualTypeArguments();
        if (index >= params.length || index < 0) {
            logger.warn("Index: " + index + ", Size of " + clazz.getSimpleName()
                    + "'s Parameterized Type: " + params.length);
            return Object.class;
        }
        if (!(params[index] instanceof Class)) {
            logger.warn(clazz.getSimpleName()
                    + " not set the actual class on superclass generic parameter");
            return Object.class;
        }
        return (Class)params[index];
    }
}
