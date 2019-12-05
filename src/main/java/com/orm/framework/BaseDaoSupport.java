package com.orm.framework;

import com.alibaba.fastjson.util.FieldInfo;
import com.alibaba.fastjson.util.TypeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.core.common.Page;
import javax.core.common.jdbc.BaseDao;
import javax.core.common.utils.BeanUtils;
import javax.core.common.utils.DataUtils;
import javax.core.common.utils.GenericsUtils;
import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * BaseDao 扩展类，主要功能是支持自动拼装sql语句，必须继承方可使用
 * 需要重写和实现以下三个方法
 * //设定主键列（这个方法在哪个父类中？？？）
 * private String getPKColumn() {return "id";}
 * //重写对象反转为Map的方法（这个方法在哪个父类中？？？）
 * protected Map<String, Object> parse(Object entity) {return utils.parse((Entity)entity);}
 * //重写结果反转为对象的方法（没重写？？？）
 * protected Entity mapRow(ResultSet rs, int rowNum) throws SQLException {return utils.parse(rs);}
 * @param <T>
 * @param <PK>
 */
public abstract class BaseDaoSupport<T extends Serializable, PK extends Serializable> implements BaseDao<T, PK> {
    private Logger log = Logger.getLogger(BaseDaoSupport.class);

    private String tableName = "";

    private JdbcTemplate jdbcTemplateWrite;

    private JdbcTemplate jdbcTemplateReadOnly;

    private DataSource dataSourceWrite;

    private DataSource dataSourceReadOnly;

    private EntityOperation<T> op;

    protected BaseDaoSupport() {
        try {
            //获取BaseDaoSupport<T extends Serializable, PK extends Serializable>中T的实际类型
            Class<T> entityClass = GenericsUtils.getSuperClassGenricType(getClass(), 0);
            op = new EntityOperation<T>(entityClass, this.getPKColumn());
            this.setTableName(op.tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected String getTableName() {
        return tableName;
    }

    /**
     * 动态切换表名
     * @param tableName
     */
    protected void setTableName(String tableName) {
        if (StringUtils.isEmpty(tableName)){
            this.tableName = op.tableName;
        } else {
            this.tableName = tableName;
        }
    }

    protected DataSource getDataSourceReadOnly() {
        return dataSourceReadOnly;
    }

    protected void setDataSourceReadOnly(DataSource dataSourceReadOnly) {
        this.dataSourceReadOnly = dataSourceReadOnly;
        jdbcTemplateReadOnly = new JdbcTemplate(dataSourceReadOnly);
    }

    protected DataSource getDataSourceWrite() {
        return dataSourceWrite;
    }

    protected void setDataSourceWrite(DataSource dataSourceWrite) {
        this.dataSourceWrite = dataSourceWrite;
        jdbcTemplateWrite = new JdbcTemplate(dataSourceWrite);
    }

    private EntityOperation<T> getOp() {
        return this.op;
    }

    private JdbcTemplate jdbcTemplateReadOnly() {
        return this.jdbcTemplateReadOnly;
    }

    private JdbcTemplate jdbcTemplateWrite() {
        return this.jdbcTemplateWrite;
    }

    /**
     * 还原默认表名
     */
    protected void restoreTableName() {
        this.setTableName(op.tableName);
    }

    /**
     * 将对象解析为Map
     */
    protected Map<String, Object> parse(T entity) {
        return op.parse(entity);
    }

    /**
     * 根据ID获取对象，如果对象不存在，返回null
     * ？？？this.jdbcTemplateReadOnly().query()方法中new HashMap<String, Object>()参数有什么用？？？
     */
    protected T get(PK id) throws Exception {
        return (T)this.doLoad(id, this.op.rowMapper);
    }

    /**
     * 获取全部对象
     * @return
     * @throws Exception
     */
    protected List<T> getAll() throws Exception {
        String sql = "select " + op.allColumn + " from " + getTableName();
        return this.jdbcTemplateReadOnly().query(sql, this.op.rowMapper, new HashMap<String, Object>());
    }

    //利用entity实例进行数据库插入
    @Override
    public PK insertAndReturnId(T entity) throws Exception {
        return (PK)this.doInsertRuturnKey(parse(entity));
    }

    @Override
    public boolean insert(T entity) throws Exception {
        return this.doInsert(parse(entity));
    }

    /**
     * 保存对象，如果对象存在则更新，否则插入
     * @param entity
     * @return
     * @throws Exception
     */
    protected boolean save(T entity) throws Exception {
        PK pkValue = (PK)op.pkField.get(entity);
        if (this.exists(pkValue)) {
            return this.doUpdate(pkValue, parse(entity)) > 0;
        } else {
            return this.doInsert(parse(entity));
        }
    }

    /**
     * 保存对象并返回id，如果对象存在则更新，否则插入
     * @param entity
     * @return
     * @throws Exception
     */
    protected PK saveAndReturnId(T entity) throws Exception {
        //Field#get(Object obj)方法返回指定对象obj上由此Field表示的字段的值
        Object o = op.pkField.get(entity);
        if (null == o) {
            return (PK)this.doInsertRuturnKey(parse(entity));
        }
        PK pkValue = (PK)o;
        if (this.exists(pkValue)){
            this.doUpdate(pkValue, parse(entity));
            return pkValue;
        } else {
            return (PK)this.doInsertRuturnKey(parse(entity));
        }
    }

    /**
     * 将对象更新到数据库
     * @param entity entity中的ID不能为空，如果ID为空，其他条件不能为空，都为空不予执行
     * @return
     * @throws Exception
     */
    @Override
    public boolean update(T entity) throws Exception {
        return this.doUpdate(op.pkField.get(entity), parse(entity)) > 0;
    }

    /**
     * 使用SQL语句更新对象
     * @param sql
     * @param args
     * @return
     * @throws Exception
     */
    protected int update(String sql, Object... args) throws Exception {
        return jdbcTemplateWrite().update(sql, args);
    }

    /**
     * 使用SQL语句更新对象。对每一页数据分别生成批量插入的sql：insert into 表名 (列1,列2...) values (值a1,值a2...),(值b1, 值b2...),...
     * @param sql
     * @param paraMap
     * @return
     * @throws Exception
     */
    protected int update(String sql, Map<String, ?> paraMap) throws Exception {
        //？？？这个Map类型的paraMap是用Object... 接收的。这是怎么处理的？？？
        return jdbcTemplateWrite().update(sql, paraMap);
    }

    @Override
    public int insertAll(List<T> list) throws Exception {
        int count = 0, len = list.size(), step = 50000;
        Map<String, PropertyMapping> pm = op.mappings;
        int maxPage = (len % step == 0) ? (len / step) : (len / step + 1);
        for (int i = 0; i <= maxPage; i++) {
            Page<T> page = pagination(list, i, step);
            String sql = "insert into " + getTableName() + "(" +op.allColumn+ ") values ";
            StringBuffer valstr = new StringBuffer();
            Object[] values = new Object[pm.size() * page.getRows().size()];
            for (int j = 0; j < page.getRows().size(); j++) {
                if (j > 0 && j < page.getRows().size()) { valstr.append(","); }
                valstr.append("(");
                int k = 0;
                for (PropertyMapping p : pm.values()) {
                    values[(j * pm.size()) + k] = p.getter.invoke(page.getRows().get(j));
                    if (k > 0 && k < pm.size()){ valstr.append(","); }
                    valstr.append("?");
                    k ++;
                }
                valstr.append(")");
            }
            int result = jdbcTemplateWrite().update(sql + valstr.toString(), values);
            count += result;
        }
        return count;
    }

    protected boolean replaceOne(T entity) throws Exception{
        return this.doReplace(parse(entity));
    }

    /**
     * 使用SQL语句更新对象。对每一页数据分别生成批量插入的sql：replace into 表名 (列1,列2...) values (值a1,值a2...),(值b1, 值b2...),...
     * @param list
     * @return
     * @throws Exception
     */
    protected int replaceAll(List<T> list) throws Exception {
        int count = 0, len = list.size(), step = 50000;
        Map<String, PropertyMapping> pm = op.mappings;
        int maxPage = (len % step == 0) ? (len / step) : (len / step + 1);
        for (int i = 0; i <= maxPage; i++) {
            Page<T> page = pagination(list, i, step);
            String sql = "replace into" + getTableName() + "(" +op.allColumn+ ") values ";
            StringBuffer valstr = new StringBuffer();
            Object[] values = new Object[pm.size() * page.getRows().size()];
            for (int j = 0; j < page.getRows().size(); j ++) {
                if(j > 0 && j < page.getRows().size()){ valstr.append(","); }
                valstr.append("(");
                int k = 0;
                for (PropertyMapping p : pm.values()) {
                    values[(j * pm.size()) + k] = p.getter.invoke(page.getRows().get(j));
                    if(k > 0 && k < pm.size()){ valstr.append(","); }
                    valstr.append("?");
                    k ++;
                }
                valstr.append(")");
            }
            int result = jdbcTemplateWrite().update(sql + valstr.toString(), values);
            count += result;
        }
        return count;
    }

    @Override
    public boolean delete(T entity) throws Exception {
        return this.doDelete(op.pkField.get(entity)) > 0;
    }

    /**
     * 根据实体的List生成批量删除sql：delete from 表名 where id in(...)
     * @param list
     * @return
     * @throws Exception
     */
    public int deleteAll(List<T> list) throws Exception {
        String pkName = op.pkField.getName();
        int count = 0, len = list.size(), step = 1000;
        Map<String, PropertyMapping> pm = op.mappings;
        int maxPage = (len % step == 0) ? (len / step) : (len / step + 1);
        for (int i = 0; i <= maxPage; i++) {
            StringBuffer valstr = new StringBuffer();
            Page<T> page = pagination(list, i, step);
            Object[] values = new Object[page.getRows().size()];
            for (int j = 0; j < page.getRows().size(); j++) {
                if (j > 0 && j < page.getRows().size()) { valstr.append(","); }
                values[j] = pm.get(pkName).getter.invoke(page.getRows().get(j));
                valstr.append("?");
            }
            String sql = "delete from " + getTableName() + " where " + pkName + " in (" +valstr.toString() + ")";
            int result = jdbcTemplateWrite.update(sql, values);
            count += result;
        }
        return count;
    }

    /**
     * 根据主键ID删除数据
     * @param id
     * @throws Exception
     */
    protected  void deleteByPK(PK id) throws Exception {
        this.doDelete(id);
    }

    protected T selectUnique(String propertyName, Object value) throws Exception {
        QueryRule queryRule = QueryRule.getInstance();
        //创建对象：查询条件
        queryRule.andEqual(propertyName, value);
        return this.selectUnique(queryRule);
    }

    protected T selectUnique(Map<String, Object> properties) throws Exception {
        QueryRule queryRule = QueryRule.getInstance();
        //将Map转换成查询条件
        for (String key : properties.keySet()) {
            queryRule.andEqual(key, properties.get(key));
        }
        return selectUnique(queryRule);
    }

    /**
     * 根据查询条件查出唯一一条记录
     * @param queryRule
     * @return
     * @throws Exception
     */
    protected T selectUnique(QueryRule queryRule) throws Exception {
        List<T> list = select(queryRule);
        if (list.size() == 0) {
            return null;
        } else if (list.size() == 1) {
            return list.get(0);
        } else {
            throw new IllegalStateException("findUnique return " +list.size() + " record(s).");
        }
    }

    /**
     * 从list中截取出需要的分页数据
     * @param objList
     * @param pageNo
     * @param pageSize
     * @return
     */
    protected Page<T> pagination(List<T> objList, int pageNo, int pageSize) {
        List<T> objectArray = new ArrayList<T>(0);
        int startIndex = (pageNo - 1) * pageSize;
        int endIndex = pageNo * pageSize;
        if (endIndex >= objList.size()) {
            endIndex = objList.size();
        }
        for (int i = startIndex; i < endIndex; i++) {
            objectArray.add(objList.get(i));
        }
        return new Page<T>(startIndex, objList.size(), pageSize, objectArray);
    }

    /**
     * 根据主键判断对象是否存在
     * @param id
     * @return
     */
    protected boolean exists(PK id) throws Exception{
        return null != this.doLoad(id, this.op.rowMapper);
    }

    /**
     * 查询满足条件的记录数
     * @param queryRule
     * @return
     * @throws Exception
     */
    protected long getCount(QueryRule queryRule) throws Exception {
        QueryRuleSqlBuilder builder = new QueryRuleSqlBuilder(queryRule);
        Object[] values = builder.getValues();
        String ws = removeFirstAnd(builder.getWhereSql());
        String whereSql = ("".equals(ws) ? ws : (" where " + ws));
        String countSql = "select count(1) from " + getTableName() + whereSql;
        return (Long) this.jdbcTemplateReadOnly().queryForMap(countSql, values).get("count(1)");
    }

    /**
     * 根据摸个属性值倒序获得第一个最大值对应的对象
     * @param propertyName
     * @return
     */
    protected  T getMax(String propertyName) throws Exception{
        QueryRule queryRule = QueryRule.getInstance();
        queryRule.addDescOrder(propertyName);
        Page<T> result = this.select(queryRule, 1, 1);
        if (null == result.getRows() || 0 == result.getRows().size()) {
            return null;
        } else {
            return result.getRows().get(0);
        }
    }

    /**
     *  将pojoList的元素复制到poList中。(如果pojo中的值为null,则继续使用po中的值）
     * @param pojoList
     * @param poList
     * @param idName
     * @throws Exception
     */
    protected void mergeList(List<T> pojoList, List<T> poList, String idName) throws Exception {
        mergeList(pojoList, poList, idName, false);
    }

    /**
     * 将pojoList的元素复制到poList中，并删除poList没有匹配上的元素，poList不增加新元素。
     * @param pojoList
     * @param poList
     * @param idName
     * @param isCopyNull
     * @throws Exception
     */
    protected void mergeList(List<T> pojoList, List<T> poList, String idName, boolean isCopyNull) throws Exception {
        Map<Object, Object> map = new HashMap<>();
        Map<String, PropertyMapping> pm = op.mappings;
        for (Object element : pojoList) {
            Object key;
            key = pm.get(idName).getter.invoke(element);
            map.put(key, element);
        }
        for (Iterator<T> it = poList.iterator(); it.hasNext();) {
            T element = it.next();
            Object key = pm.get(idName).getter.invoke(element);
            if (!map.containsKey(key)) {
                delete(element);
                it.remove();
            } else {
                DataUtils.copySimpleObject(map.get(key), element, isCopyNull);
            }
        }
    }

    /**
     * 将ResultSet转换成T实例对象
     * @param rs
     * @param obj
     * @param <T>
     * @return
     */
    private <T> T populate(ResultSet rs, T obj) {
        try {
            ResultSetMetaData metaData = rs.getMetaData(); //结果集的元数据
            int colCount = metaData.getColumnCount();
            Field[] fields = obj.getClass().getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field f = fields[i];
                for (int j = 1; j <= colCount; j++) {
                    // 注意：rs的游标从1开始
                    Object value = rs.getObject(j);
                    String colName = metaData.getColumnName(j);
                    if (!f.getName().equalsIgnoreCase(colName)) {
                        continue;
                    }
                    //如果列名和字段名一样，则给实体对象该字段设置值
                    BeanUtils.copyProperty(obj, f.getName(),value);
                }
            }
        } catch (Exception e) {
            log.warn("populate error..." + e);
        }
        return obj;
    }

    /**
     * 封装一下JdbcTemplate的queryForObject方法。查询出唯一结果，查询出多个抛出异常
     * @param sql
     * @param mapper
     * @param args
     * @param <T>
     * @return
     */
    private <T> T selectForObject(String sql, RowMapper<T> mapper, Object... args) {
        List<T> results = this.jdbcTemplateReadOnly().query(sql, mapper, args);
        return DataAccessUtils.singleResult(results);
    }

    /**
     * 将Blob类型的数值（转换成InputStream，再）转换成byte[]类型
     * @param rs
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    protected byte[] getBlobColumn(ResultSet rs, int columnIndex) throws SQLException {
        try {
            Blob blob = rs.getBlob(columnIndex);
            if (blob == null) {
                return null;
            }
            InputStream is = blob.getBinaryStream();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (is == null) {
                return null;
            } else {
                //InputStream#read(byte[] b) 和 ByteArrayOutputStream#write(...)
                //InputStream#read() 这个方法是对这个输入流一个一个字节的读，返回的int就是这个字节的int表示方式。因为每次只能读取一个字节，所以也只能读取由ASCII码范围内的一些字符。这些字符主要用于显示现代英语和其他西欧语言。
                //InputStream#read(byte[] b) 这个方法是先规定一个数组长度，将这个流中的字节缓冲到数组b中，返回的这个数组中的字节个数，这个缓冲区没有满的话，则返回真实的字节个数，到未尾时都返回-1。如果文件保存了10个字节的数据，而bytes长度为8，那么inputStream会按照每8个字节读一次文件，在此例中会读取两次，并且最终输出结果会有问题。这是因为第二次读取出来的两个字节会按照读取顺序依次填充在bytes数组的前两位，而后面6位元素并不会改变。
                //在使用InputStream读取文件时，发现在使用while循环读取文件时，它会自动的按顺序读取文件的内容
                //ByteArrayOutputStream#write(int w) 将指定的字节（w是这个字节的int表示方式）写入此字节数组输出流。
                //ByteArrayOutputStream#write(byte []b, int off, int len) 将指定字节数组中从偏移量 off 开始的 len 个字节写入此字节数组输出流。
                byte buffer[] = new byte[64];
                int c= is.read(buffer);
                while (c > 0) {
                    bos.write(buffer, 0, c);
                    c = is.read(buffer);
                }
                return bos.toByteArray();
            }
        } catch (IOException e) {
            throw new SQLException("Failed to read BLOB column due to IOException: " + e.getMessage());
        }
    }

    /**
     * 将预编译的sql中某参数值设置成Blob类型（代码：PreparedStatement#setBinaryStream）
     * @param stmt
     * @param parameterIndex
     * @param value
     * @throws SQLException
     */
    protected void setBlobColumn(PreparedStatement stmt, int parameterIndex, byte[] value) throws SQLException {
        if (value == null) {
            stmt.setNull(parameterIndex, Types.BLOB);
        } else {
            stmt.setBinaryStream(parameterIndex, new ByteArrayInputStream(value), value.length);
        }
    }

    /**
     * 将Clob类型的数值（转换成InputStream，再）转换成String
     * @param rs
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    protected String getClobColumn(ResultSet rs, int columnIndex) throws SQLException {
        try {
            Clob clob = rs.getClob(columnIndex);
            if (clob == null) {
                return null;
            }
            StringBuffer ret = new StringBuffer();
            InputStream is = clob.getAsciiStream();
            if (is == null) {
                return null;
            } else {
                byte buffer[] = new byte[64];
                int c= is.read(buffer);
                while (c > 0) {
                    ret.append(new String(buffer,0,c));
                    c = is.read(buffer);
                }
                return ret.toString();
            }
        } catch (IOException e) {
            //？？？catch的是IOException，throw的是SQLException，不合适吧？？？
            throw new SQLException("Failed to read CLOB column due to IOException: " + e.getMessage());
        }
    }

    /**
     * 将预编译的sql中某参数值设置成Clob类型（代码：PreparedStatement#setAsciiStream）
     * @param stmt
     * @param parameterIndex
     * @param value
     * @throws SQLException
     */
    protected void setClobColumn(PreparedStatement stmt, int parameterIndex, String value) throws SQLException {
        if (value == null) {
            stmt.setNull(parameterIndex, Types.CLOB);
        } else {
            stmt.setAsciiStream(parameterIndex, new ByteArrayInputStream(value.getBytes()), value.length());
        }
    }

    private Page simplePageQuery(String sql, RowMapper<T> rowMapper, Map<String, ?> args, long pageNo, long pageSize) {
        long start = (pageNo - 1) * pageSize;
        return simplePageQueryByStart(sql, rowMapper, args, start, pageSize);
    }

    /**
     * 分页查询支持，支持简单的sql查询分页（复杂的查询，请自行编写对应的方法），Map<String,?> args是查询条件，返回Page
     * @param sql
     * @param rowMapper
     * @param args
     * @param pageNo
     * @param pageSize
     * @return
     */
    private Page simplePageQueryByStart(String sql, RowMapper<T> rowMapper, Map<String,?> args, long pageNo, long pageSize) {
        // 查询总数
        String countSql = "select count(*) " + removeSelect(removeOrders(sql));
        //？？？countSql中是count(*)，这里get("count(1)")，不对吧？？？
        long count = (Long)this.jdbcTemplateReadOnly.queryForMap(countSql, args).get("count(1)");
        if (count == 0) {
            log.debug("no result...");
            return new Page();
        }
        long start = (pageNo - 1) * pageSize;
        sql = sql + " limit " + start + "," + pageSize;
        log.debug(javax.core.common.utils.StringUtils.format("[Execute SQL]sql:{0},params:{1}", sql, args));
        List list = this.jdbcTemplateReadOnly.query(sql, rowMapper, args);
        return new Page(start, count, (int)pageSize, list);
    }

    /**
     * 获取查询sql的记录数
     * @param sql
     * @param args
     * @return
     */
    protected long queryCount(String sql, Map<String, ?> args) {
        String countSql = "select count(1) " + removeSelect(removeOrders(sql));
        return (Long)this.jdbcTemplateReadOnly().queryForMap(countSql, args).get("count(1)");
    }

    /**
     * 分页查询支持，支持简单的sql查询分页（复杂的查询，请自行编写对应的方法），返回List
     * @param sql
     * @param rowMapper
     * @param args
     * @param start
     * @param pageSize
     * @param <T>
     * @return
     */
    //？？？这个方法声明中最前面的<T>写与不写有无区别？有没有什么时候必须写？？？？
    protected <T> List<T> simpleListQueryByStart(String sql, RowMapper<T> rowMapper, Map<String,?> args, long start, long pageSize) {
        sql = sql + " limit " + start + "," + pageSize;
        log.debug(javax.core.common.utils.StringUtils.format("[Execute SQL:{0},params:{1}]",sql,args));
        List<T> list = this.jdbcTemplateReadOnly().query(sql, rowMapper, args);
        if (list == null) {
            return new ArrayList<>();
        }
        return list;
    }

    /**
     * 分页查询支持，支持简单的sql查询分页（复杂的查询，请自行编写对应的方法）。不带泛型。
     * @param sql
     * @param rm
     * @param args
     * @param pageNo
     * @param pageSize
     * @return
     */
    private Page simplePageQueryNotT(String sql, RowMapper rm, Map<String, ?> args, long pageNo, long pageSize) {
        String countSql = "select count(*) " + removeSelect(removeOrders(sql));
        long count = (Long)this.jdbcTemplateReadOnly().queryForMap(countSql, args).get("count(1)");
        if (count == 0) {
            log.debug("no result...");
            return new Page();
        }
        long start = (pageNo - 1) * pageSize;
        sql = sql + " limit " + start + "," + pageSize;
        log.debug(javax.core.common.utils.StringUtils.format("[Execute SQL]sql:{0},params:{1}",sql, args));
        List list = this.jdbcTemplateReadOnly().query(sql, rm, args);
        return new Page(start, count, (int)pageSize, list);
    }

    /**
     * 去掉sql中的排序语句片段
     * @param sql
     * @return
     */
    private String removeOrders(String sql) {
        Pattern p = Pattern.compile("order\\s*by[\\w|\\W|\\s|\\S]*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        StringBuffer sb = new StringBuffer();
        while (m.find()){
            m.appendReplacement(sb, "");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * 去掉sql中from及from前面语句片段
     * @param sql
     * @return
     */
    private String removeSelect(String sql) {
        int beginPos = sql.toLowerCase().indexOf("from");
        return sql.substring(beginPos);
    }

    /**
     * 获取某表某列的最大值
     * @param table
     * @param column
     * @return
     */
    private long getMaxId(String table, String column){
        String sql = "SELECT mmax(" + column + ") FROM " + table + " ";
        long maxId = (Long)this.jdbcTemplateReadOnly().queryForMap(sql).get("max(" + column + ")");
        return maxId;
    }

    /**
     * 根据sql查出唯一一条记录
     * @param sql
     * @param param Map，key为属性名，value为属性值
     * @return
     * @throws Exception
     */
    protected Map<String, Object> selectUniqueBySql(String sql, Map<String, ?> param) throws Exception {
        List<Map<String, Object>> list = selectBySql(sql, param);
        if (list.size() == 0) {
            return null;
        } else if (list.size() == 1) {
            return list.get(0);
        } else {
            throw new IllegalStateException("findUnique return " + list.size() + " record(s).");
        }
    }

    /**
     * 根据sql查出唯一一条记录
     * @param sql
     * @param args 为Object数组
     * @return
     * @throws Exception
     */
    protected Map<String, Object> selectUniqueBySql(String sql, Object... args) throws Exception {
        List<Map<String, Object>> list = selectBySql(sql, args);
        if (list.size() == 0) {
            return null;
        } else if (list.size() == 1) {
            return list.get(0);
        } else {
            throw new IllegalStateException("findUnique return " + list.size() + " record(s).");
        }
    }

    /**
     * 根据sql查出唯一一条记录
     * @param sql
     * @param listParam 属性值list
     * @return
     * @throws Exception
     */
    protected Map<String, Object> selectUniqueBySql(String sql, List<Object> listParam) throws Exception {
        List<Map<String, Object>> listMap = selectBySql(sql, listParam);
        if (listMap.size() == 0) {
            return null;
        } else if (listMap.size() == 1) {
            return listMap.get(0);
        } else {
            throw new IllegalStateException("findUnique return " + listMap.size() + " record(s).");
        }
    }

    private int doUpdate(String tableName, String pkName, Object pkValue, Map<String, Object> params){
        params.put(pkName, pkValue);
        String sql = this.makeSimpleUpdateSql(tableName, pkName, pkValue, params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret;
    }

    private int doUpdate(String pkName, Object pkValue, Map<String, Object> params){
        params.put(pkName, pkValue);
        String sql = this.makeSimpleUpdateSql(pkName, pkValue, params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret;
    }

    /**
     * 更新实体对象，返回更新记录数量
     * @param pkValue
     * @param params
     * @return
     */
    private int doUpdate(Object pkValue, Map<String, Object> params) {
        String sql = this.makeSimpleUpdateSql(pkValue, params);
        //？？？这个是不是多余的，pkName：pkValue的键值对已经有了吧？？？
        params.put(this.getPKColumn(), pkValue);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret;
    }

    private boolean doReplace(Map<String, Object> params) {
        String sql = this.makeSimpleReplaceSql(this.getTableName(), params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret > 0;
    }

    private boolean doReplace(String tableName, Map<String, Object> params){
        String sql = this.makeSimpleReplaceSql(tableName, params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret > 0;
    }

    private boolean doInsert(String tableName, Map<String, Object> params){
        String sql = this.makeSimpleInsertSql(tableName, params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret > 0;
    }

    //执行insert语句，返回true/false
    private boolean doInsert(Map<String, Object> params) {
        String sql = this.makeSimpleInsertSql(this.getTableName(), params);
        int ret = this.jdbcTemplateWrite().update(sql, params.values().toArray());
        return ret > 0;
    }

    //执行insert语句，并返回主键值
    private Serializable doInsertRuturnKey(Map<String, Object> params) {
        final List<Object> values = new ArrayList<>();
        final String sql = makeSimpleInsertSql(getTableName(), params, values);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSourceWrite());
        jdbcTemplate.update(new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                for (int i = 0; i < values.size(); i++) {
                    ps.setObject(i+1, values.get(i)==null?null:values.get(i));
                }
                return ps;
            }
        }, keyHolder);

        if (keyHolder == null) {return "";}
        Map<String, Object> keys = keyHolder.getKeys();
        if (keys == null || keys.size() == 0 || keys.values().size() == 0) {
            return "";
        }
        Object key = keys.values().toArray()[0];
        if (key == null || !(key instanceof Serializable)) {
            return "";
        }
        if (key instanceof Number) {
            Class clazz = key.getClass();
            return (clazz == int.class || clazz == Integer.class) ? ((Number)key).intValue() : ((Number)key).longValue();
        } else if (key instanceof String) {
            return (String) key;
        } else {
            return (Serializable)key;
        }
    }

    private String makeSimpleUpdateSql(Object pkValue, Map<String, Object> params) {
        return this.makeSimpleUpdateSql(getTableName(), getPKColumn(), pkValue, params);
    }

    /**
     * （1）生成简单对象UPDATE语句，简化sql拼接，where条件是id，params是需要赋的值（2）给params增加一个数据where_id:具体值。？？？怎么用？？？
     * @param tableName
     * @param pkName
     * @param pkValue
     * @param params
     * @return
     */
    private String makeSimpleUpdateSql(String tableName, String pkName, Object pkValue, Map<String, Object> params) {
        if (StringUtils.isEmpty(tableName) || params == null || params.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("update ").append(tableName).append(" set ");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sb.append(key).append(" = ?");
            if (index != set.size() - 1) {
                sb.append(",");
            }
            index++;
        }
        sb.append(" where ").append(pkName).append(" = ?");
        //？？？怎么用？？？
        params.put("where_" + pkName, params.get(pkName));
        return sb.toString();
    }

    /**
     * 生成简单对象UPDATE语句，简化sql拼接，where条件是id，params是需要赋的值
     * @param pkName
     * @param pkValue
     * @param params
     * @return
     */
    private String makeSimpleUpdateSql(String pkName, Object pkValue, Map<String, Object> params){
        if (StringUtils.isEmpty(getTableName()) || params == null || params.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("update ").append(getTableName()).append(" set ");
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sb.append(key).append(" = :").append(key);
            if (index != set.size() - 1){
                sb.append(",");
            }
            index++;
        }
        sb.append(" where ").append(pkName).append(" = :").append(pkName);
        return sb.toString();
    }

    /**
     * 生成简单REPLACE语句，简化sql拼接，占位符用的是冒号加列名。
     * @param tableName
     * @param params
     * @return
     */
    //REPLACE的运行与INSERT很相似。只有一点例外，假如表中的一个旧记录与一个用于PRIMARY KEY或一个UNIQUE索引的新记录具有相同的值，则在新记录被插入之前，旧记录被删除。
    private String makeSimpleReplaceSql(String tableName, Map<String, Object> params) {
        if(StringUtils.isEmpty(tableName) || params == null || params.isEmpty()){
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("replace into ").append(tableName);
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();
        sbKey.append("(");
        sbValue.append("(");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sbKey.append(key);
            sbValue.append(" :").append(key);
            if (index != set.size() - 1) {
                sbKey.append(",");
                sbValue.append(",");
            }
            index++;
        }
        sbKey.append(")");
        sbValue.append(")");
        sb.append(sbKey).append("VALUES").append(sbValue);
        return sb.toString();
    }

    /**
     * 生成简单REPLACE语句，简化sql拼接，占位符用的是问号。并将Map<String, Object> params的每个值添加到List<Object> values中
     * @param tableName
     * @param params
     * @param values
     * @return
     */
    private String makeSimpleReplaceSql(String tableName, Map<String, Object> params, List<Object> values) {
        if (StringUtils.isEmpty(tableName) || params == null || params.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("repalace into ").append(tableName);
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();
        sbKey.append("(");
        sbValue.append("(");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sbKey.append(key);
            sbValue.append(" ?");
            if (index != set.size() - 1) {
                sbKey.append(",");
                sbValue.append(",");
            }
            index++;
            values.add(params.get(key));
        }
        sbKey.append(")");
        sbValue.append(")");
        sb.append(sbKey).append("VALUES").append(sbValue);
        return sb.toString();
    }

    /**
     * 生成对象INSERT语句，简化sql拼接
     * @param tableName
     * @param params
     * @return
     */
    private String makeSimpleInsertSql(String tableName, Map<String, Object> params) {
        if (StringUtils.isEmpty(tableName) || params == null || params.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("insert into ").append(tableName);
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();
        sbKey.append("(");
        sbValue.append("(");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sbKey.append(key);
            sbValue.append(" ?");
            if (index != set.size() - 1) {
                sbKey.append(",");
                sbValue.append(",");
            }
            index++;
        }
        sbKey.append(")");
        sbValue.append(")");
        sb.append(sbKey).append("VALUES").append(sbValue);
        return sb.toString();
    }

    /**
     * （1）生成对象INSERT语句，简化sql拼接，（2）给values赋值（获取的是对象的属性值）
     * @param tableName
     * @param params
     * @param values
     * @return
     */
    private String makeSimpleInsertSql(String tableName, Map<String, Object> params, List<Object> values) {
        if (StringUtils.isEmpty(tableName) || params == null || params.isEmpty()) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        sb.append("insert into ").append(tableName);
        StringBuffer sbKey = new StringBuffer();
        StringBuffer sbValue = new StringBuffer();
        sbKey.append("(");
        sbValue.append("(");
        //添加参数
        Set<String> set = params.keySet();
        int index = 0;
        for (String key : set) {
            sbKey.append(key);
            sbValue.append(" ?");
            if (index != set.size() - 1) {
                sbKey.append(",");
                sbValue.append(",");
            }
            index++;
            values.add(params.get(key));
        }
        sbKey.append(")");
        sbValue.append(")");
        sb.append(sbKey).append("VALUES").append(sbValue);
        return sb.toString();
    }

    private String makeDefaultSimpleUpdateSql(Object pkValue, Map<String, Object> params) {
        return this.makeSimpleUpdateSql(getTableName(), getPKColumn(), pkValue, params);
    }

    private String makeDefaultSimpleInsertSql(Map<String, Object> params){
        return this.makeSimpleInsertSql(this.getTableName(), params);
    }

    private <T> T doLoad(Object pkValue, RowMapper<T> rowMapper) {
        Object obj = this.doLoad(getTableName(), getPKColumn(), pkValue, rowMapper);
        if (obj != null) {
            return (T)obj;
        }
        return null;
    }

    /**
     * 查询数据库，获取一个实体对象
     * @param tableName
     * @param pkName
     * @param pkValue
     * @param rm
     * @return
     */
    private Object doLoad(String tableName, String pkName, Object pkValue, RowMapper rm) {
        StringBuffer sb = new StringBuffer();
        sb.append("select * from ").append(tableName).append(" where ").append(pkName).append(" = ? ");
        List<Object> list = this.jdbcTemplateReadOnly().query(sb.toString(), rm, pkValue);
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    private int doDelete(Object pkValue) {
        return this.doDelete(getTableName(), getPKColumn(), pkValue);
    }

    /**
     * 根据主键删除数据，返回删除的记录数
     * @param tableName
     * @param pkName
     * @param pkValue
     * @return
     */
    private int doDelete(String tableName, String pkName, Object pkValue) {
        StringBuffer sb = new StringBuffer();
        sb.append("delete from ").append(tableName).append(" where ").append(pkName).append(" = ?");
        int ret = this.jdbcTemplateWrite.update(sb.toString(), pkValue);
        return ret;
    }

    /**
     * 结合查询条件、排序生成sql语句，进行单表查询
     * @param queryRule 查询条件
     * @return
     * @throws Exception
     */
    @Override
    public List<T> select(QueryRule queryRule) throws Exception {
        QueryRuleSqlBuilder builder = new QueryRuleSqlBuilder(queryRule);
        String ws = removeFirstAnd(builder.getWhereSql());
        String whereSql = ("".equals(ws) ? ws : (" where " + ws));
        String sql = "select " + op.allColumn + " from " + getTableName() + whereSql;
        Object[] values = builder.getValues();
        String orderSql = builder.getOrderSql();
        orderSql = (StringUtils.isEmpty(orderSql) ? " " : (" order by " + orderSql));
        sql += orderSql;
        log.debug(sql);
        return (List<T>) this.jdbcTemplateReadOnly().query(sql, this.op.rowMapper, values);
    }



    //去掉最前面的and
    private String removeFirstAnd(String sql) {
        if (StringUtils.isEmpty(sql)) {return sql;}
        return sql.trim().toLowerCase().replaceAll("^\\s*and","") + " ";
    }


    /**
     * 结合查询条件、排序和分页信息生成SQL，并查询出分页数据
     * @param queryRule 查询条件
     * @param pageNo 页码
     * @param pageSize 每页条数
     * @return
     * @throws Exception
     */
    @Override
    public Page<T> select(QueryRule queryRule, final int pageNo, final int pageSize) throws Exception {
        QueryRuleSqlBuilder builder = new QueryRuleSqlBuilder(queryRule);
        Object[] values = builder.getValues();
        String ws = removeFirstAnd(builder.getWhereSql());
        String whereSql = ("".equals(ws) ? ws : (" where " + ws));
        String countSql = "select count(1) from " + getTableName() + whereSql;
        long count = (Long) this.jdbcTemplateReadOnly().queryForMap(countSql, values).get("count(1)");
        if (count == 0) {
            return new Page<>();
        }
        long start = (pageNo - 1) * pageSize;
        // 有数据的情况下，继续查询
        String orderSql = builder.getOrderSql();
        orderSql = (StringUtils.isEmpty(orderSql) ? " " : (" order by " + orderSql));
        String sql = "select " + op.allColumn + " from " + getTableName() + whereSql + orderSql + " limit " + start + "," + pageSize;
        List<T> list = (List<T>) this.jdbcTemplateReadOnly().query(sql, this.op.rowMapper, values);
        log.debug(sql);
        return new Page<T>(start, count, pageSize, list);
    }

    /**
     * 根据SQL语句查询出对象集合
     * @param sql
     * @param args 为Object数组
     * @return
     * @throws Exception
     */
    @Override
    public List<Map<String, Object>> selectBySql(String sql, Object... args) throws Exception {
        return this.jdbcTemplateReadOnly().queryForList(sql, args);
    }

    /**
     * 根据SQL语句查询出对象集合
     * @param sql
     * @param param 为Map，key为属性名，value为属性值
     * @return
     * @throws Exception
     */
    protected List<Map<String, Object>> selectBySql(String sql, Map<String,?> param) throws Exception {
        return this.jdbcTemplateReadOnly().queryForList(sql, param);
    }

    /**
     * 根据SQL语句查询出对象集合
     * @param sql
     * @param list
     * @return
     * @throws Exception
     */
    protected List<Map<String, Object>> selectBySql(String sql, List<Object> list) throws Exception {
        return this.jdbcTemplateReadOnly().queryForList(sql, list.toArray());
    }

    /**
     * 根据SQl语句，加上查询条件、页码和页容量查出分页数据
     * @param sql SQL语句
     * @param param
     * @param pageNo 页码
     * @param pageSize 每页条数
     * @return
     * @throws Exception
     */
    @Override
    public Page<Map<String, Object>> selectBySqlToPage(String sql, Object[] param, int pageNo, int pageSize) throws Exception {
        String countSql = "select count(1) from (" + sql + ") a";
        long count = (Long) this.jdbcTemplateReadOnly().queryForMap(countSql, param).get("count(1)");
        if (count == 0) {
            return new Page<>();
        }
        long start = (pageNo - 1) * pageSize;
        sql = sql + " limit " + start + "," + pageSize;
        List<Map<String, Object>> list = this.jdbcTemplateReadOnly().queryForList(sql, param);
        log.debug(sql);
        return new Page<>(start, count, pageSize, list);
    }

    /**
     * 根据SQl语句，加上查询条件、页码和页容量查出分页数据
     * @param sql
     * @param param
     * @param pageNo
     * @param pageSize
     * @return
     * @throws Exception
     */
    protected Page<Map<String, Object>> selectBySqlToPage(String sql, Map<String,?> param, final int pageNo, final int pageSize) throws Exception {
        String countSql = "select count(1) from (" + sql + ") a";
        long count = (Long) this.jdbcTemplateReadOnly().queryForMap(countSql, param).get("count(1)");
        if (count == 0) {
            return new Page<>();
        }
        long start = (pageNo - 1) * pageSize;
        //有数据的情况下，继续查询
        sql = sql + " limit " + start + "," + pageSize;
        List<Map<String, Object>> list = this.jdbcTemplateReadOnly().queryForList(sql, param);
        log.debug(sql);
        return new Page<>(start, count, pageSize, list);
    }

    @Override
    public int deleleAll(List<T> list) throws Exception {
        return 0;
    }

    /**
     * 获取主键列名称，建议子类重写
     * @return
     */
    protected abstract String getPKColumn();

    protected abstract void setDataSource(DataSource dataSource);

    /**
     * 将Object对象转换成Map（属性:属性值）
     * @param obj
     * @return
     */
    private Map<String, Object> convertMap(Object obj){
        Map<String,Object> map = new HashMap<>();
        List<FieldInfo> getters = TypeUtils.computeGetters(obj.getClass(), null);
        for (int i = 0,len=getters.size(); i < len; i++) {
            FieldInfo fieldInfo = getters.get(i);
            String name = fieldInfo.getName();
            try {
                Object value = fieldInfo.get(obj);
                map.put(name, value);
            } catch (Exception e) {
                log.error(String.format("covertMap error object:%s field: %s",obj.toString(),name));
            }
        }
        return map;
    }

}
