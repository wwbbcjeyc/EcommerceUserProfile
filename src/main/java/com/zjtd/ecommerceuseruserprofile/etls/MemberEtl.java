package com.zjtd.ecommerceuseruserprofile.etls;

import com.alibaba.fastjson.JSON;
import com.zjtd.ecommerceuseruserprofile.utlis.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        // 写sql查询数据
        List<MemberSex> memberSexes = memberSexEtl(session);
        List<MemberChannel> memberChannels = memberChannelEtl(session);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(session);
        MemberHeat memberHeat = memberHeatEtl(session);

        // 拼成需要的结果
        MemberVo memberVo = new MemberVo();
        memberVo.setMemberSexes(memberSexes);
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberHeat(memberHeat);

        //打印到控制台输出
        System.out.println("===========" + JSON.toJSONString(memberVo));
    }

    public static List<MemberSex> memberSexEtl(SparkSession session) {
        // 先用sql得到每个性别的count统计数据
        Dataset<Row> dataset = session.sql(
                "select sex as memberSex, count(id) as sexCount " +
                        " from ecommerce.t_member group by sex");

        List<String> list = dataset.toJSON().collectAsList();

        // 对每一个元素依次map成MemberSex，收集起来
        List<MemberSex> result = list.stream()
                .map( str -> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());

        return result;
    }

    public static List<MemberChannel> memberChannelEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql(
                "select member_channel as memberChannel, count(id) as channelCount " +
                        " from ecommerce.t_member group by member_channel");

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberChannel> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());

        return result;
    }

    public static List<MemberMpSub> memberMpSubEtl(SparkSession session) {
        Dataset<Row> sub = session.sql(
                "select count(if(mp_open_id !='null',true,null)) as subCount, " +
                        " count(if(mp_open_id ='null',true,null)) as unSubCount " +
                        " from ecommerce.t_member");

        List<String> list = sub.toJSON().collectAsList();

        List<MemberMpSub> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());

        return result;
    }

    public static MemberHeat memberHeatEtl(SparkSession session) {
        // reg , complete , order , orderAgain, coupon
        Dataset<Row> reg_complete = session.sql(
                "select count(if(phone='null',true,null)) as reg," +
                        " count(if(phone !='null',true,null)) as complete " +
                        " from ecommerce.t_member");

        Dataset<Row> order_again = session.sql(
                "select count(if(t.orderCount =1,true,null)) as order," +
                        "count(if(t.orderCount >=2,true,null)) as orderAgain from " +
                        "(select count(order_id) as orderCount,member_id from ecommerce.t_order group by member_id) as t");

        Dataset<Row> coupon = session.sql("select count(distinct member_id) as coupon from ecommerce.t_coupon_member ");

        // 最终，将三张表（注册、复购、优惠券）连在一起
        Dataset<Row> heat = coupon.crossJoin(reg_complete).crossJoin(order_again);

        List<String> list = heat.toJSON().collectAsList();
        List<MemberHeat> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberHeat.class))
                .collect(Collectors.toList());

        // 只有一行数据，获取后返回
        return result.get(0);
    }

    // 想要展示饼图的数据信息
    @Data
    static class MemberVo{
        private List<MemberSex> memberSexes;    // 性别统计信息
        private List<MemberChannel> memberChannels;  // 渠道来源统计信息
        private List<MemberMpSub> memberMpSubs;  // 用户是否关注媒体平台
        private MemberHeat memberHeat;   // 用户热度统计
    }
    // 分别定义每个元素类
    @Data
    static class MemberSex {
        private Integer memberSex;
        private Integer sexCount;
    }
    @Data
    static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;
    }
    @Data
    static class MemberMpSub {
        private Integer subCount;
        private Integer unSubCount;
    }
    @Data
    static class MemberHeat {
        private Integer reg;    // 只注册，未填写手机号
        private Integer complete;    // 完善了信息，填了手机号
        private Integer order;    // 下过订单
        private Integer orderAgain;    // 多次下单，复购
        private Integer coupon;    // 购买过优惠券，储值
    }
}

