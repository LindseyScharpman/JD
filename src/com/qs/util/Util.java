package com.qs.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * write by lhg
 */
public class Util {
    private static Util ourInstance = new Util();
    private Date start;

    public DateFormat getDf() {
        return df;
    }

    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static Util getInstance() {
        return ourInstance;
    }

    private Util() {
        try {
            start = df.parse("2016-02-01 00:00:00");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public int getSign(long v) {
        if (v < 0) return 0;
        return 1;
    }

    public int getDeltaDate(String end) throws ParseException {
        Date date2 = df.parse(end);
        long diff = date2.getTime() - start.getTime();
        int days = (int) (diff / (1000 * 60 * 60 * 24));
        return days;
    }

    public int getDeltaDate(Date end) throws ParseException {
        long diff = end.getTime() - start.getTime();
        int days = (int) (diff / (1000 * 60 * 60 * 24));
        return days;
    }

    public long getDeltaTime(Date end) {
        return end.getTime() - start.getTime();
    }

    public BufferedReader getReader(String f) throws Exception {
        return new BufferedReader(new FileReader(f));
    }

    public BufferedWriter getWriter(String f) throws Exception {
        return new BufferedWriter(new FileWriter(f));
    }

    public float getRate(float fenzi, float fenmu) {
        return fenzi / (fenmu + 0.01f);
    }

    public Set<String> getProduct() throws Exception {
        BufferedReader br = getReader(Constant.DIR + "JData_Product.csv");
        String line = br.readLine();
        Set<String> set = new HashSet<>();
        while ((line = br.readLine()) != null) {
            String splits[] = line.split(",");
            set.add(splits[0]);
        }
        br.close();
        return set;
    }

    public static void main(String[] args) throws ParseException {
        Util util = Util.getInstance();
        System.out.println(util.getDeltaDate("2016-04-15 00:00:00"));
    }
}
