package com.qs.data;

import com.qs.util.Constant;
import com.qs.util.Util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * -Xms4096m
 * -Xmx4096m
 */
public class UserItemFeature {
    private Util util = Util.getInstance();
    private Map<String, long[][]> userCounts;
    private int cnt = 0;

    public static void main(String[] args) throws Exception {
        // 只从2-1到4-15号中取出交易记录的商品id在P集合中的样本
        // P集合是  JData_Product的所有sku_id
        UserItemFeature uif = new UserItemFeature();
//        uif.productData(Constant.DIR + "Product_JData_Action.csv");
//
//        // 把Product_JData_Action拆分成10个小文件 users_0.csv....
//        uif.mapSplit();


        for (int i = 0; i < 10; i++)
            uif.extract(Constant.DIR_SPLIT + "users_" + i + ".csv",
                    Constant.DIR_SPLIT + "useritem_features_" + i + "_75d.csv",
                    75);
    }


    // select_days:75天 2.1---4.15
    public void extract(String input, String output, int select_days) throws Exception {
        userCounts = new HashMap<>(); // user_id + sku_id + brand字符串做为键

        System.out.println(input);
        BufferedReader br = util.getReader(input); // input ： users_0.csv...
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");

            String uid = splits[0].substring(0, splits[0].length() - 2);
            // splits[1] : sku_id 商品id
            // splits[6] : brand 品牌id
            // 值是75x14的矩阵 75天X(6种点击类型的次数 + 6种点击类型与2.1差距的最长分钟数 (同一天内) )
            long[][] counts = userCounts.getOrDefault(uid + "," + splits[1] + "," + splits[6], new long[Constant.DAYS][7 * 2]);
            Date date = util.getDf().parse(splits[2]);
            int index = util.getDeltaDate(date);    // 返回date和2.1相差N天
            if (index < 0) {
                System.out.println("fatal error" + line); // 根本不可能
                continue;
            }

            // index : 和2.1相差N天,最大相差75天
            // splits[4] : 行为数据中“点击的类型” type，取值范围是1-6
            counts[index][Integer.parseInt(splits[4])] += 1;
            //counts[index][Integer.parseInt(splits[4]) + 7] 是对应点击类型的时间与2.1差距的分钟数(同一天内)
            if (counts[index][Integer.parseInt(splits[4]) + 7] < util.getDeltaTime(date) / 1000 / 60) // < 和2.1差距的分钟数
            {
                counts[index][Integer.parseInt(splits[4]) + 7] = util.getDeltaTime(date) / 1000 / 60;
                cnt++; // 71W+
            }
            userCounts.put(uid + "," + splits[1] + "," + splits[6], counts);
        }

        br.close();
        BufferedWriter bw = util.getWriter(output); // useritem_features_0_75d
        StringBuffer sb = new StringBuffer();

        int[] intervals = new int[]{1, 7, 15, 30, 60};
        sb.append("user,item,brand,which"); // item 就是skuid
        for (int n = 0; n < intervals.length; n++) {
            for (int type = 1; type <= 6; type++) // 6种类型
                sb.append(",uif_" + type + "_" + n);
            if (n > 1) {
                sb.append(",uif_rate_4_1_" + n);
                sb.append(",uif_rate_4_2_" + n);
                sb.append(",uif_rate_4_6_" + n);
            }
        }
        sb.append(",uif_earliest");
        sb.append(",uif_online_days");
        sb.append(",uif_rate_e_online_days");
        sb.append(",label");
        bw.write(sb.toString() + "\r\n");
        bw.flush();

        // 值是75x14的矩阵 75天X(6种点击类型的次数 + 6种点击类型与2.1差距的最长分钟数 (同一天内) )
        // uib : useid,skuid,brand
        for (String uib : userCounts.keySet()) {
            long[][] counts = userCounts.get(uib);
            for (int i = 0; i <= 10; i++) {
                int label = 0;
                int earliest = 1;
                int label_start = 70 - (i - 1) * Constant.window_interval; //  Constant.window_interval值是1
                if (i == 0)
                    label_start = 75;
                int label_end = label_start + 4;
                int select_end = label_start - 1;
                int select_start = select_end - select_days + 1;
                for (int j = label_start; j <= label_end; j++)
                    if (j < counts.length && counts[j][4] > 0) label = 1;
                sb = new StringBuffer();
                int sum = 0; // 行为次数和
                sb.append(uib + "," + i); // i是which字段
                for (int n = 0; n < intervals.length; n++) {
                    int typeCounts[] = new int[7];
                    for (int j = label_start - 1; j >= label_start - intervals[n] && j >= 0; j--) {
                        int daysum = 0;
                        for (int type = 1; type <= 6; type++) {
                            typeCounts[type] += counts[j][type];
                            daysum += counts[j][type];
                        }
                        sum += daysum;
                    }
                    for (int type = 1; type <= 6; type++)
                        sb.append("," + typeCounts[type]);
                    if (n > 1) {
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[1]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[2]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[6]));
                    }
                }

                int online_days = 0;
                for (int j = label_start - 1; j >= 0; j--) {
                    int day_sum = 0;
                    for (int type = 1; type <= 6; type++) {
                        if (counts[j][type] == 0)
                            continue;
                        day_sum += counts[j][type];
                    }
                    if (day_sum > 0) {
                        online_days++; // 在线的天数，如果当天没有行为数据则不在线
                        if ((label_start - j) > earliest) // earliest 距离lable_start最大的天数
                            earliest = label_start - j;
                    }
                }
                if (sum == 0) { // 在各个区间内都没有历史行为，则忽略当前样本
                    System.out.println(uib );
                    continue;
                }
                sb.append("," + earliest);
                sb.append("," + online_days);
                sb.append("," + util.getRate(earliest, online_days));
                sb.append("," + label);
                bw.write(sb.toString() + "\r\n");
                bw.flush();
            }
        }
        bw.close();
    }

    public void mapSplit() throws Exception {
        BufferedWriter bws[] = new BufferedWriter[10];
        for (int i = 0; i < bws.length; i++) {
            bws[i] = util.getWriter(Constant.DIR_SPLIT + "users_" + i + ".csv");
            bws[i].write("user_id,sku_id,time,model_id,type,cate,brand\r\n");
            bws[i].flush();
        }
        String fileName = "Product_JData_Action.csv";
        BufferedReader br = util.getReader(Constant.DIR + fileName);
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            bws[(int) (Float.parseFloat(splits[0]) % 10)].write(line + "\r\n");
            bws[(int) (Float.parseFloat(splits[0]) % 10)].flush();
        }
        for (int i = 0; i < bws.length; i++)
            bws[i].close();
        br.close();
    }

    private void productData(String f) throws Exception {
        Set<String> products = util.getProduct();
        BufferedWriter bw = util.getWriter(f);
        for (int i = 2; i <= 4; i++) {
            String fileName = "JData_Action_20160" + i + ".csv";
            BufferedReader br = util.getReader(Constant.DIR + fileName);
            String line = br.readLine();
            if (i == 2) {
                bw.write(line + "\r\n");
                bw.flush();
            }
            while ((line = br.readLine()) != null) {
                String splits[] = line.split(",");
                if (!products.contains(splits[1])) continue;
                bw.write(line + "\r\n");
                bw.flush();
            }
            br.close();
        }
        bw.close();
    }

}
