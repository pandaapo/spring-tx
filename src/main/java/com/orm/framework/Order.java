package com.orm.framework;

/**
 * sql排序组件
 */
public class Order {
    private boolean ascending; //升序还是降序
    private String propertyName; //哪个字段升序，哪个字段降序

    @Override
    public String toString() {
        return propertyName + ' ' + (ascending ? "asc" : "desc");
    }

    public Order(String propertyName, boolean ascending) {
        this.ascending = ascending;
        this.propertyName = propertyName;
    }

    public static Order asc(String propertyName) {
        return new Order(propertyName, true);
    }

    public static Order desc(String propertyName) {
        return new Order(propertyName, false);
    }
}
