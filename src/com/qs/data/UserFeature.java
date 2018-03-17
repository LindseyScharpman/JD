package com.qs.data;

import com.qs.util.Constant;
import com.qs.util.Util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.*;


public class UserFeature {
    private Map<String, long[][]> userCounts;
    private Util util = Util.getInstance();

    public static void main(String[] args) throws Exception {
        UserFeature uf = new UserFeature();
        uf.userInfoFeature(Constant.DIR + "user_info.csv"); // age sex level

        uf.extract(Constant.DIR + "user_features_75d.csv", 75);

        for (int i = 0; i < 10; i++)
            uf.extractLabel(Constant.DIR_SPLIT + "users_" + i + ".csv", Constant.DIR_SPLIT + "user_features_" + i + "_75d.csv", 75);
        for (int i = 0; i < 10; i++)
            uf.extractProduct(Constant.DIR_SPLIT + "users_" + i + ".csv", Constant.DIR_SPLIT + "user_count_features_" + i + "_75d.csv", 75);
    }

    public UserFeature() throws Exception {
    }

    public void userInfoFeature(String f) throws Exception {
        BufferedReader br = util.getReader(Constant.DIR + "JData_User.csv");
        String line = br.readLine();
        Map<String, Integer> map1 = new HashMap<>();
        Map<String, Integer> map2 = new HashMap<>();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            map1.put(splits[1], map1.getOrDefault(splits[1], map1.size() + 1));
            map2.put(splits[2], map2.getOrDefault(splits[2], map2.size() + 1));
        }
        br.close();
        System.out.println(map1.size());
        System.out.println(map2.size());
        int arrays1[] = new int[map1.size()];
        int arrays2[] = new int[map2.size()];
        br = util.getReader(Constant.DIR + "JData_User.csv");
        BufferedWriter bw = util.getWriter(f);
        line = br.readLine();
        int n = 0;
        StringBuffer sb = new StringBuffer();
        sb.append("user");
        for (int i = 0; i < arrays1.length; i++)
            sb.append(",user_age_" + i);
//        sb.append(",user_age");
        for (int i = 0; i < arrays2.length; i++)
            sb.append(",user_sex_" + i);
        sb.append(",level\r\n");
        while ((line = br.readLine()) != null) {
            Arrays.fill(arrays1, 0);
            Arrays.fill(arrays2, 0);
            String splits[] = line.split(",");
            sb.append(splits[0]);
            arrays1[map1.get(splits[1]) - 1] = 1;
            arrays2[map2.get(splits[2]) - 1] = 1;
//            int age = 0;
//            if (splits[1].compareTo("NULL") != 0 && splits[1].compareTo("-1") != 0)
//                age = Integer.parseInt(splits[1].substring(0, 2));
//            sb.append("," + age);
            for (int v : arrays1)
                sb.append("," + v);
            for (int v : arrays2)
                sb.append("," + v);
            sb.append("," + splits[3] + "\r\n");
            n++;
            if (n == 100) {
                bw.write(sb.toString());
                bw.flush();
                n = 0;
                sb = new StringBuffer();
            }
        }
        br.close();
        bw.close();
    }

    public void extractLabel(String input, String output, int select_days) throws Exception {
        System.out.println(input);
        Map<String, Set[][]> userBehavSet = new HashMap<>();
        BufferedReader br = Util.getInstance().getReader(input);
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            // [0]: userid
            Set[][] counts = userBehavSet.getOrDefault(splits[0], new Set[Constant.DAYS][7 * 2]);
            Date date = util.getDf().parse(splits[2]);
            int index = util.getDeltaDate(date);
            if (index < 0) continue;
            if (counts[index][Integer.parseInt(splits[4])] == null) // type
                counts[index][Integer.parseInt(splits[4])] = new HashSet();
            if (counts[index][Integer.parseInt(splits[4]) + 7] == null) // type距离2.1
                counts[index][Integer.parseInt(splits[4]) + 7] = new HashSet();
            counts[index][Integer.parseInt(splits[4])].add(splits[1]); // skuid
            counts[index][Integer.parseInt(splits[4]) + 7].add(splits[6]); // brand
            userBehavSet.put(splits[0], counts);
        }
        br.close();
        BufferedWriter bw = util.getWriter(output);
        StringBuffer sb = new StringBuffer();
        int[] intervals = new int[]{1, 7, 15, 30, 60};
        sb.append("user,which");
        for (int n = 0; n < intervals.length; n++) {
            for (int type = 1; type <= 6; type++) {
                sb.append(",product_f_" + type + "_" + n);
                sb.append(",product_brand_f_" + type + "_" + n);
            }
        }
        sb.append(",product_f_earliest");
        sb.append(",product_uf_online_days");
        sb.append(",product_uf_rxh");
        sb.append(",product_uf_rate_e_on");
        sb.append(",label");
        bw.write(sb.toString() + "\r\n");
        bw.flush();
        for (String user : userBehavSet.keySet()) {
            Set[][] counts = userBehavSet.get(user);
            for (int i = 0; i <= 5; i++) {
                int label = 0;
                int earliest = 1;
                int label_start = (Constant.DAYS - 5) - (i - 1) * Constant.window_interval;
                if (i == 0)
                    label_start = Constant.DAYS;
                int label_end = label_start + 4;
                int select_end = label_start - 1;
                int select_start = select_end - select_days + 1;
                for (int j = label_start; j <= label_end; j++) {
                    if (j < counts.length && counts[j][4] != null && counts[j][4].size() > 0) label = 1;
                }
                sb = new StringBuffer();
                int sum = 0;
                sb.append(user + "," + i);
                for (int n = 0; n < intervals.length; n++) {
                    int typeCounts[] = new int[7];
                    int brandCounts[] = new int[7];
                    for (int j = label_start - 1; j >= label_start - intervals[n] && j >= 0; j--) {
                        for (int type = 1; type <= 6; type++) {
                            if (counts[j][type] == null) continue;
                            typeCounts[type] += counts[j][type].size();
                            brandCounts[type] += counts[j][type + 7].size();
                            if (j >= select_start && j <= select_end) sum += counts[j][type].size();
                        }
                    }
                    for (int type = 1; type <= 6; type++) {
                        sb.append("," + typeCounts[type]);
                        sb.append("," + brandCounts[type]);
                    }
                }
                int online_days = 0;
                int rxh = 1;
                for (int j = label_start - 1; j >= 0; j--) {
                    int day_sum = 0;
                    for (int type = 1; type <= 6; type++) {
                        if (counts[j][type] == null) continue;
                        day_sum += counts[j][type].size();
                    }
                    if (day_sum > 0) {
                        online_days++;
                        if ((label_start - j) > earliest) earliest = label_start - j;
                        rxh *= (label_start - j);
                    }
                }
                if (sum == 0) continue;
                sb.append("," + earliest);
                sb.append("," + online_days);
                sb.append("," + rxh);
                sb.append("," + util.getRate(earliest, online_days));
                sb.append("," + label);
                bw.write(sb.toString() + "\r\n");
                bw.flush();
            }
        }
        bw.close();
    }

    public void extractProduct(String input, String output, int select_days) throws Exception {
        System.out.println(input);
        userCounts = new HashMap<>();
        BufferedReader br = Util.getInstance().getReader(input);
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            long[][] counts = userCounts.getOrDefault(splits[0], new long[Constant.DAYS][7]);
            Date date = util.getDf().parse(splits[2]);
            int index = util.getDeltaDate(date);
            if (index < 0) continue;
            counts[index][Integer.parseInt(splits[4])] += 1;
            userCounts.put(splits[0], counts);
        }
        br.close();
        BufferedWriter bw = util.getWriter(output);
        StringBuffer sb = new StringBuffer();
        int[] intervals = new int[]{1, 7, 15, 30, 60};
        sb.append("user,which");
        for (int n = 0; n < intervals.length; n++) {
            for (int type = 1; type <= 6; type++)
                sb.append(",product_count_f_" + type + "_" + n);
        }
        bw.write(sb.toString() + "\r\n");
        bw.flush();
        for (String user : userCounts.keySet()) {
            long[][] counts = userCounts.get(user);
            for (int i = 0; i <= 5; i++) {
                int label_start = (Constant.DAYS - 5) - (i - 1) * Constant.window_interval;
                if (i == 0)
                    label_start = Constant.DAYS;
                int select_end = label_start - 1;
                int select_start = select_end - select_days + 1;
                sb = new StringBuffer();
                int sum = 0;
                sb.append(user + "," + i);
                for (int n = 0; n < intervals.length; n++) {
                    int typeCounts[] = new int[7];
                    for (int j = label_start - 1; j >= label_start - intervals[n] && j >= 0; j--) {
                        int daysum = 0;
                        for (int type = 1; type <= 6; type++) {
                            typeCounts[type] += counts[j][type];
                            if (j >= select_start && j <= select_end) daysum += counts[j][type];
                        }
                        sum += daysum;
                    }
                    for (int type = 1; type <= 6; type++)
                        sb.append("," + typeCounts[type]);
                }
                if (sum == 0) continue;
                bw.write(sb.toString() + "\r\n");
                bw.flush();
            }
        }
        bw.close();
    }

    // select_days 75
    public void extract(String f, int select_days) throws Exception {
        userCounts = new HashMap<>();
        for (int i = 2; i <= 4; i++) {
            String fileName = "JData_Action_20160" + i + ".csv"; // 读取所有行为信息,而不是只出现在P集合(被预测的商铺集合)种数据
            BufferedReader br = Util.getInstance().getReader(Constant.DIR + fileName);
            String line = br.readLine();
            while ((line = br.readLine()) != null) {
                String splits[] = line.split(",");
                long[][] counts = userCounts.getOrDefault(splits[0], new long[Constant.DAYS][7]);
                Date date = util.getDf().parse(splits[2]);
                int index = util.getDeltaDate(date);
                if (index < 0) continue;
                counts[index][Integer.parseInt(splits[4])] += 1;
                userCounts.put(splits[0], counts);
            }
            br.close();
        }
        BufferedWriter bw = util.getWriter(f);
        StringBuffer sb = new StringBuffer();
        int[] intervals = new int[]{1, 7, 15, 30, 60};
        sb.append("user,which");
        for (int n = 0; n < intervals.length; n++) {
            for (int type = 1; type <= 6; type++)
                sb.append(",f_" + type + "_" + n);
            if (n > 1) {
                sb.append(",rate_4_1_" + n);
                sb.append(",rate_4_2_" + n);
                sb.append(",rate_4_6_" + n);
                sb.append(",buydays_" + n);
                sb.append(",onlinedays_" + n); // 该区间内在线次数
                sb.append(",rate_b_on_" + n); // 该区间内buydays / onlinedays
            }
        }
        sb.append(",lastbuydays");
        sb.append(",uf_earliest");
        sb.append(",uf_online_days");
        sb.append(",uf_rxh");
        sb.append(",uf_rate_e_on");
        bw.write(sb.toString() + "\r\n");
        bw.flush();
        for (String user : userCounts.keySet()) {
            long[][] counts = userCounts.get(user);
            for (int i = 0; i <= 5; i++) {
                int label_start = (Constant.DAYS - 5) - (i - 1) * Constant.window_interval;
                if (i == 0)
                    label_start = Constant.DAYS;
                int select_end = label_start - 1;
                int select_start = select_end - select_days + 1;
                sb = new StringBuffer();
                int sum = 0;
                int earliest = 1;
                sb.append(user + "," + i);
                int lastbuydays = 60;
                for (int n = 0; n < intervals.length; n++) {
                    int typeCounts[] = new int[7];
                    int buydays = 0;
                    int onlinedays = 0;
                    for (int j = label_start - 1; j >= label_start - intervals[n] && j >= 0; j--) {
                        int daysum = 0;
                        for (int type = 1; type <= 6; type++) {
                            typeCounts[type] += counts[j][type];
                            if (j >= select_start && j <= select_end) daysum += counts[j][type];
                        }
                        sum += daysum;
                        if (counts[j][4] > 0) {
                            buydays++;
                            if (lastbuydays == 60)
                                lastbuydays = j;
                        }
                        if (daysum > 0)
                            onlinedays++;
                    }
                    for (int type = 1; type <= 6; type++)
                        sb.append("," + typeCounts[type]);
                    if (n > 1) {
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[1]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[2]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[6]));
                        sb.append("," + buydays);
                        sb.append("," + onlinedays);
                        sb.append("," + util.getRate(buydays, onlinedays));
                    }
                }
                int online_days = 0;
                int rxh = 1;
                for (int j = label_start - 1; j >= 0; j--) {
                    int day_sum = 0;
                    for (int type = 1; type <= 6; type++) {
                        if (counts[j][type] == 0) continue;
                        day_sum += counts[j][type];
                    }
                    if (day_sum > 0) {
                        online_days++;
                        if ((label_start - j) > earliest)
                            earliest = label_start - j;
                        rxh *= (label_start - j);
                    }
                }
                sb.append("," + lastbuydays);
                sb.append("," + earliest);
                sb.append("," + online_days);
                sb.append("," + rxh);
                sb.append("," + util.getRate(earliest, online_days));
                if (sum == 0) continue;
                bw.write(sb.toString() + "\r\n");
                bw.flush();
            }
        }
        bw.close();
    }

}
