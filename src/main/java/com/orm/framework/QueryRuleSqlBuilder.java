package com.orm.framework;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orm.framework.QueryRule.Rule;

/**
 * SQL构造器
 */
public class QueryRuleSqlBuilder {
    private int CURR_INDEX = 0; //记录参数所在的位置
    private List<String> properties; //保存列名列表
    private List<Object> values; //保存参数值列表
    private List<Order> orders; //保存排序规则列表
    private String whereSql = "";
    private String orderSql = "";
    private Object [] valueArr = new Object[]{};
    private Map<Object, Object> valueMap = new HashMap<>();

    public Object [] getValues() {
        return valueArr;
    }

    public String getWhereSql() {
        return whereSql;
    }

    public String getOrderSql() {
        return orderSql;
    }

    public Map<Object, Object> getValueMap() {
        return valueMap;
    }

    /**
     * 创建SQL构造器
     */
    public QueryRuleSqlBuilder(QueryRule queryRule) {
        CURR_INDEX = 0;
        properties = new ArrayList<>();
        values = new ArrayList<>();
        orders = new ArrayList<>();
        for (Rule rule : queryRule.getRuleList()) {
            switch (rule.getType()) {
                case QueryRule.BETWEEN :
                    processBetween(rule);
                    break;
                case QueryRule.EQ:
                    processEqual(rule);
                    break;
                case QueryRule.LIKE:
                    processLike(rule);
                    break;
                case QueryRule.NOTEQ:
                    processNotEqual(rule);
                    break;
                case QueryRule.GT:
                    processGreaterThen(rule);
                    break;
                case QueryRule.GE:
                    processGreaterEqual(rule);
                    break;
                case QueryRule.LT:
                    processLessThen(rule);
                case QueryRule.LE:
                    processLessEqual(rule);
                    break;
                case QueryRule.IN:
                    processIN(rule);
                    break;
                case QueryRule.NOTIN:
                    processNotIN(rule);
                    break;
                case QueryRule.ISNULL:
                    processIsNull(rule);
                    break;
                case QueryRule.ISNOTNULL:
                    processIsNotNull(rule);
                    break;
                case QueryRule.ISEMPTY:
                    processIsEmpty(rule);
                    break;
                case QueryRule.ISNOTEMPTY:
                    processIsNotEmpty(rule);
                    break;
                case QueryRule.ASC_ORDER:
                    processOrder(rule);
                    break;
                case QueryRule.DESC_ORDER:
                    processOrder(rule);
                    break;
                default:
                    throw new IllegalArgumentException("type " + rule.getType() + " not supported.");
            }
        }
        //拼装where语句
        appendWhereSql();
        //拼装排序语句
        appendOrderSql();
        //拼装参数值
        appendValues();
    }

    /**
     * 处理between
     * @param rule
     */
    private void processBetween(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues()) || rule.getValues().length < 2) {
            return;
        }
        add(rule.getAndOr(), rule.getPropertyName(), "", "between", rule.getValues()[0], "add");
    }

    /**
     * 处理=
     * @param rule
     */
    private void processEqual(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        add(rule.getAndOr(), rule.getPropertyName(),"=",rule.getValues()[0]);
    }

    /**
     * 处理 like
     * @param rule
     */
    private void processLike(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        Object obj = rule.getValues()[0];
        if (obj != null) {
            String value = obj.toString();
            if (!StringUtils.isEmpty(value)) {
                // ？？？什么意思？？？
                value = value.replace('*','%');
                obj = value;
            }
        }
        add(rule.getAndOr(), rule.getPropertyName(), "like", "%" + rule.getValues()[0] + "%");
    }

    /**
     * 处理<>
     * @param rule
     */
    private void processNotEqual(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        add(rule.getAndOr(), rule.getPropertyName(), "<>", rule.getValues()[0]);
    }

    /**
     * 处理>
     * @param rule
     */
    private void processGreaterThen(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        add(rule.getAndOr(), rule.getPropertyName(), ">", rule.getValues()[0]);
    }

    /**
     * 处理>=
     * @param rule
     */
    private void processGreaterEqual(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        add(rule.getAndOr(), rule.getPropertyName(), ">=", rule.getValues()[0]);
    }

    /**
     * 处理<
     * @param rule
     */
    private  void processLessThen(Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        add(rule.getAndOr(),rule.getPropertyName(),"<",rule.getValues()[0]);
    }

    /**
     * 处理<=
     * @param rule
     */
    private  void processLessEqual(
            Rule rule) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        add(rule.getAndOr(),rule.getPropertyName(),"<=",rule.getValues()[0]);
    }

    /**
     * 处理 is null
     * @param rule
     */
    private void processIsNull(Rule rule) {
        add(rule.getAndOr(), rule.getPropertyName(), "is null", null);
    }

    /**
     * 处理 is not null
     * @param rule
     */
    private void processIsNotNull(Rule rule) {
        add(rule.getAndOr(), rule.getPropertyName(), "is not null", null);
    }

    /**
     * 处理 <>''
     * @param rule
     */
    private void processIsNotEmpty(Rule rule) {
        add(rule.getAndOr(), rule.getPropertyName(), "<>", "''");
    }

    /**
     * 处理 =''
     * @param rule
     */
    private  void processIsEmpty(Rule rule) {
        add(rule.getAndOr(),rule.getPropertyName(),"=","''");
    }

    /**
     * 处理in和not in
     * @param rule
     * @param name
     */
    private void inAndNotIn(Rule rule, String name) {
        if (ArrayUtils.isEmpty(rule.getValues())) {
            return;
        }
        if ((rule.getValues().length == 1) && (rule.getValues()[0] != null) && (rule.getValues()[0] instanceof List)){
            List<Object> list = (List) rule.getValues()[0];
            if ((list != null) && (list.size() > 0)) {
                for (int i = 0; i < list.size(); i++) {
                    if (i == 0 && i == list.size() - 1) {
                        add(rule.getAndOr(), rule.getPropertyName(), "", name + "(", list.get(i),")");
                    } else if (i == 0 && i < list.size() - 1) {
                        add(rule.getAndOr(), rule.getPropertyName(),"",name + "(", list.get(i), "");
                    }
                    if (i > 0 && i < list.size() - 1) {
                        add(0,"",",","", list.get(i), "");
                    }
                    if (i == list.size() - 1 && i != 0) {
                        add(0,"",",","",list.get(i),")");
                    }
                }
            }
        } else {
            Object[] list = rule.getValues();
            for (int i = 0; i < list.length; i++) {
                if (i == 0 && i == list.length - 1) {
                    add(rule.getAndOr(), rule.getPropertyName(), "",name + "(", list[i], ")");
                } else if (i == 0 && i < list.length - 1) {
                    add(rule.getAndOr(), rule.getPropertyName(), "", name + "(", list[i], "");
                }
                if (i > 0 && i < list.length - 1) {
                    add(0,"",",","",list[i],"");
                }
                if (i == list.length - 1 && i != 0) {
                    add(0,"",",","",list[i],")");
                }
            }
        }
    }

    /**
     * 处理 not in
     * @param rule
     */
    private void processNotIN(Rule rule){
        inAndNotIn(rule,"not in");
    }

    /**
     * 处理 in
     * @param rule
     */
    private  void processIN(Rule rule) {
        inAndNotIn(rule,"in");
    }

    /**
     * 处理 order by
     * @param rule
     */
    private void processOrder(Rule rule) {
        switch (rule.getType()) {
            case QueryRule.ASC_ORDER:
                if (!StringUtils.isEmpty(rule.getPropertyName())) {
                    orders.add(Order.asc(rule.getPropertyName()));
                }
                break;
            case QueryRule.DESC_ORDER:
                if (!StringUtils.isEmpty(rule.getPropertyName())) {
                    orders.add(Order.desc(rule.getPropertyName()));
                }
                break;
            default:
                break;
        }
    }

    private void add(int andOr, String key, String split, Object value) {
        add(andOr, key, split, "", value, "");
    }

    /**
     * 拼接sql的where查询条件语句；构建两个数组：List<String> properties where查询语句片段列表（例如 and 列名 > 0）和List<Object> values参数值列表
     * @param andOr
     * @param key
     * @param split
     * @param prefix
     * @param value
     * @param suffix
     */
    private void add(int andOr, String key, String split, String prefix, Object value, String suffix) {
        String andOrStr = (0 == andOr ? "" : (QueryRule.AND == andOr ? " and " : " or "));
        properties.add(CURR_INDEX, andOrStr + key + " " + split + prefix + (null != value ? " ? " : " ") + suffix);
        if (null != value) {
            values.add(CURR_INDEX, value);
            CURR_INDEX ++;
        }
    }

    /**
     * 拼装 where 语句
     */
    private void appendWhereSql(){
        StringBuffer whereSql = new StringBuffer();
        for (String p : properties) {
          whereSql.append(p);
        }
        this.whereSql = removeSelect(removeOrders(whereSql.toString()));

    }

    /**
     * 去掉select
     * @param sql
     * @return
     */
    private String removeSelect(String sql) {
        if (sql.toLowerCase().matches("from\\s+")) {
            int beginPos = sql.toLowerCase().indexOf("from");
            return sql.substring(beginPos);
        } else {
            return sql;
        }
    }

    /**
     * 去掉order
     * @param sql
     * @return
     */
    private String removeOrders(String sql) {
        Pattern p = Pattern.compile("order\\s*by[\\w|\\W|\\s|\\S]*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, "");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * 拼装排序语句
     */
    private void appendOrderSql(){
        StringBuffer orderSql = new StringBuffer();
        for (int i = 0; i < orders.size(); i++) {
            if (i > 0 && i < orders.size()) {
                orderSql.append(",");
            }
            orderSql.append(orders.get(i).toString());
        }
        this.orderSql = removeSelect(removeOrders(orderSql.toString()));
    }

    /**
     * 拼装参数值：将List<Object>的values转成一个Object[]，一个Map<Object, Object>
     */
    private void appendValues(){
        Object [] val = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            val[i] = values.get(i);
            valueMap.put(i, values.get(i));
        }
        this.valueArr = val;
    }
}
