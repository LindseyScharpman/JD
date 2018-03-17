package com.qs.data;

import com.qs.util.Constant;
import com.qs.util.Util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class BrandFeature {
    private Map<String, long[][]> brandCounts;
    private Util util = Util.getInstance();

    public static void main(String[] args) throws Exception {
        BrandFeature feature = new BrandFeature();
        feature.extract(Constant.DIR + "brand_features_75d.csv", 75);
    }


    public void extract(String f, int select_days) throws Exception {
        brandCounts = new HashMap<>();
        String fileName = "Product_JData_Action.csv";
        BufferedReader br = Util.getInstance().getReader(Constant.DIR + fileName);
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            long[][] counts = brandCounts.getOrDefault(splits[6], new long[Constant.DAYS][7]);
            Date date = util.getDf().parse(splits[2]);
            int index = util.getDeltaDate(date);
            if (index < 0) continue;
            counts[index][Integer.parseInt(splits[4])] += 1;
            brandCounts.put(splits[6], counts);
        }
        br.close();
        BufferedWriter bw = util.getWriter(f);
        StringBuffer sb = new StringBuffer();
        int[] intervals = new int[]{1, 7, 15, 30, 60};
        sb.append("brand,which");
        for (int n = 0; n < intervals.length; n++) {
            for (int type = 1; type <= 6; type++)
                sb.append(",brand_f_" + type + "_" + n);
            if (n > 1) {
                sb.append(",brand_rate_4_1_" + n);
                sb.append(",brand_rate_4_2_" + n);
                sb.append(",brand_rate_4_6_" + n);
//                sb.append(",brand_buydays_" + n);
            }
        }
        bw.write(sb.toString() + "\r\n");
        bw.flush();
        for (String user : brandCounts.keySet()) {
            long[][] counts = brandCounts.get(user);
            for (int i = 0; i <= 30; i++) {
                int label_start = 70 - (i - 1) * Constant.window_interval;
                if (i == 0)
                    label_start = 75;
                int select_end = label_start - 1;
                int select_start = select_end - select_days + 1;
                sb = new StringBuffer();
                int sum = 0;
                sb.append(user + "," + i);
                for (int n = 0; n < intervals.length; n++) {
                    int typeCounts[] = new int[7];
                    int buydays = 0;
                    for (int j = label_start - 1; j >= label_start - intervals[n] && j >= 0; j--) {
                        int daysum = 0;
                        for (int type = 1; type <= 6; type++) {
                            typeCounts[type] += counts[j][type];
                            if (j >= select_start && j <= select_end) daysum += counts[j][type];
                        }
                        sum += daysum;
                        if (counts[j][4] > 0) {
                            buydays++;
                        }
                    }
                    for (int type = 1; type <= 6; type++)
                        sb.append("," + typeCounts[type]);
                    if (n > 1) {
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[1]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[2]));
                        sb.append("," + util.getRate(typeCounts[4], typeCounts[6]));
//                        sb.append("," + buydays);
                    }
                }
                if (sum == 0) continue;
                bw.write(sb.toString() + "\r\n");
                bw.flush();
            }
        }
        bw.close();
    }

}
