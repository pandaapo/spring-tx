package com.orm.transaction.dao;

import com.orm.framework.BaseDaoSupport;
import com.orm.framework.QueryRule;
import com.orm.transaction.entity.Member;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.List;

public class MemberDao_myOrm extends BaseDaoSupport<Member, Long> {
    @Override
    protected String getPKColumn() {
        return "id";
    }

    //固定数据源
    @Resource(name="dataSource")
    @Override
    protected void setDataSource(DataSource dataSource) {
        super.setDataSourceReadOnly(dataSource);
        super.setDataSourceWrite(dataSource);
    }

    /**
     * 根据name左包含“破”该查询条件查询
     * @return
     * @throws Exception
     */
    public List<Member> selectAll() throws Exception {
        QueryRule queryRule = QueryRule.getInstance();
        queryRule.andLike("name","破%");
        return super.select(queryRule);
    }
}
