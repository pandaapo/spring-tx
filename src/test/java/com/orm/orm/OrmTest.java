package com.orm.orm;

import com.orm.transaction.dao.MemberDao;
import com.orm.transaction.dao.OrderDao;
import com.orm.transaction.entity.Member;
import com.orm.transaction.entity.Order;
import org.aspectj.weaver.ast.Or;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@ContextConfiguration(locations = {"classpath:application-context.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class OrmTest {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmdd");

    @Autowired
    private MemberDao memberDao;

    @Autowired
    private OrderDao orderDao;

    @Test
    public void testSelectAllForMember(){
        try {
            List<Member> result = memberDao.selectAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void testInsertMember(){
        try {
            for (int age = 25; age < 35; age++) {
                Member member = new Member();
                member.setAge(age);
                member.setName("吴");
                member.setAddr("兴");
                memberDao.insert(member);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
//    @Ignore
    public void testInsertOrder(){
        try {
            Order order = new Order();
            order.setMemberId(1L);
            order.setDetail("历史订单");
            Date date = sdf.parse("20180201123456");
            order.setCreateTime(date.getTime());
            orderDao.insertOne(order);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
