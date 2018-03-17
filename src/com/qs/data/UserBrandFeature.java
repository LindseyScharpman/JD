package com.qs.data;

import com.qs.util.Constant;
import com.qs.util.Util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class UserBrandFeature {
    private Util util = Util.getInstance();
    private Map<String, long[][]> userCounts;

    public static void main(String[] args) throws Exception {
        UserBrandFeature uif = new UserBrandFeature();
        for (int i = 0; i < 10; i++)
            uif.extract(Constant.DIR_SPLIT + "users_" + i + ".csv", Constant.DIR_SPLIT + "userbrand_features_" + i + "_75d.csv", 75);
    }

    public void extract(String input, String output, int select_days) throws Exception {
        userCounts = new HashMap<>();
        System.out.println(input);
        BufferedReader br = util.getReader(input);
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            long[][] counts = userCounts.getOrDefault(splits[0] + "," + splits[6], new long[Constant.DAYS][7 * 2]);
            Date date = util.getDf().parse(splits[2]);
            int index = util.getDeltaDate(date);
            if (index < 0) continue;
            counts[index][Integer.parseInt(splits[4])] += 1;
            if (counts[index][Integer.parseInt(splits[4]) + 7] < util.getDeltaTime(date) / 1000 / 60)
                counts[index][Integer.parseInt(splits[4]) + 7] = util.getDeltaTime(date) / 1000 / 60;
            userCounts.put(splits[0] + "," + splits[6], counts);
        }
        br.close();
        BufferedWriter bw = util.getWriter(output);
        StringBuffer sb = new StringBuffer();
        int[] intervals = new int[]{1, 7, 15, 30, 60};
        sb.append("user,brand,which");
        for (int n = 0; n < intervals.length; n++) {
            for (int type = 1; type <= 6; type++)
                sb.append(",ubf_" + type + "_" + n);
            if (n > 1) {
                sb.append(",ubf_rate_4_1_" + n);
                sb.append(",ubf_rate_4_2_" + n);
                sb.append(",ubf_rate_4_6_" + n);
//                sb.append(",ubf_buydays_" + n);
//                sb.append(",ubf_onlinedays_" + n);
//                sb.append(",ubf_rate_b_on_" + n);
            }
        }
//        sb.append(",ubf_lastbuydays");
//        for (int type = 1; type <= 2; type++)
//            sb.append(",ubf_max_h_" + type);
        sb.append(",ubf_earliest");
        sb.append(",ubf_online_days");
        sb.append(",ubf_rate_e_online_days");
        bw.write(sb.toString() + "\r\n");
        bw.flush();
        for (String uib : userCounts.keySet()) {
            long[][] counts = userCounts.get(uib);
            for (int i = 0; i <= 10; i++) {
                int earliest = 1;
                int label_start = 70 - (i - 1) * Constant.window_interval;
                if (i == 0)
                    label_start = 75;
                int select_end = label_start - 1;
                int select_start = select_end - select_days + 1;
                sb = new StringBuffer();
                int sum = 0;
                sb.append(uib + "," + i);
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
                            if (lastbuydays == 60) lastbuydays = j;
                        }
                        if (daysum > 0) {
                            onlinedays++;
                            if (label_start - j > earliest) earliest = label_start - j;
                        }
                    }
                    for (int type = 1; type <= 6; type++)
                        sb.append("," + typeCounts[type]);
                    if (n > 1) {
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[1]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[2]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[6]));
//                        sb.append("," + buydays);
//                        sb.append("," + onlinedays);
//                        sb.append("," + util.getRate(buydays, onlinedays));
                    }
                }
                int online_days = 0;
                for (int j = label_start - 1; j >= 0; j--) {
                    int day_sum = 0;
                    for (int type = 1; type <= 6; type++) {
                        if (counts[j][type] == 0) continue;
                        day_sum += counts[j][type];
                    }
                    if (day_sum > 0) {
                        online_days++;
                        if ((label_start - j) > earliest) earliest = label_start - j;
                    }
                }
//                sb.append("," + lastbuydays);
//                sb.append("," + util.getSign(counts[select_start][4 + 7] - counts[select_start][2 + 7]));
//                sb.append("," + util.getSign(counts[select_start][3 + 7] - counts[select_start][2 + 7]));
                if (sum == 0) continue;
                sb.append("," + earliest);
                sb.append("," + online_days);
                sb.append("," + util.getRate(earliest, online_days));
                bw.write(sb.toString() + "\r\n");
                bw.flush();
            }
        }
        bw.close();
    }
}
