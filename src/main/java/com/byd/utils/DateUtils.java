package com.byd.utils;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author wang.yonggui1
 * @date 2021-12-10 9:40
 */
public class DateUtils extends org.apache.commons.lang3.time.DateUtils {

    public static String[] PARSE_PATTERNS = {
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM",
            "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm", "yyyy/MM",
            "yyyy.MM.dd", "yyyy.MM.dd HH:mm:ss", "yyyy.MM.dd HH:mm", "yyyy.MM",
            "yyyy-MM-dd HH:mm:ss.SSS"};

    public static Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static Date add(Date date, TimeUnit unit, long amount) {
        try {
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(Calendar.MILLISECOND, (int) unit.toMillis(amount));
            return c.getTime();
        } catch (Exception e) {
            return null;
        }
    }
    public static Date add(Date date, int field, int amount) {
        try {
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(field, amount);
            return c.getTime();
        } catch (Exception e) {
            return null;
        }
    }

    public static Timestamp add(Timestamp date, int field, int amount) {
        try {
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(field, amount);
            return new Timestamp(c.getTimeInMillis());
        } catch (Exception e) {
            return null;
        }

    }

    public static Date addYear(Date date, int amount) {
        return add(date, Calendar.YEAR, amount);
    }

    public static Date addMonth(Date date, int amount) {
        return add(date, Calendar.MONTH, amount);
    }

    public static Date addDay(Date date, int amount) {
        return add(date, Calendar.DATE, amount);
    }

    public static Date addWeek(Date date, int amount) {
        return add(date, Calendar.WEEK_OF_YEAR, amount);
    }

    public static Date addHour(Date date, int amount) {
        return add(date, Calendar.HOUR, amount);
    }

    public static Date addMinute(Date date, int amount) {
        return add(date, Calendar.MINUTE, amount);
    }

    public static Date addSecond(Date date, int amount) {
        return add(date, Calendar.SECOND, amount);
    }

    /**
     * date1 - date2的时间差
     *
     * @param date1
     * @param date2
     * @param unit
     * @return
     */
    public static long getTimeDiff(Date date1, Date date2, TimeUnit unit) {
        if (date1 == null || date2 == null) {
            throw new IllegalArgumentException("date1:" + date1 + ", date2:" + date2 + " both cannot be null!!");
        }
        return unit.convert(date1.getTime() - date2.getTime(), TimeUnit.MILLISECONDS);
    }

    public static long getTimeDiff(Timestamp date1, Timestamp date2, TimeUnit unit) {
        if (date1 == null || date2 == null) {
            throw new IllegalArgumentException("date1:" + date1 + ", date2:" + date2 + " both cannot be null!!");
        }
        return unit.convert(date1.getTime() - date2.getTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * 得到当前日期字符串 格式（yyyy-MM-dd）
     */
    public static String getDate() {
        return getDate("yyyy-MM-dd");
    }

    /**
     * 得到当前日期字符串 格式（yyyy-MM-dd） pattern可以为："yyyy-MM-dd" "HH:mm:ss" "E"
     */
    public static String getDate(String pattern) {
        return DateFormatUtils.format(new Date(), pattern);
    }

    /**
     * 得到日期字符串 默认格式（yyyy-MM-dd） pattern可以为："yyyy-MM-dd" "HH:mm:ss" "E"
     */
    public static String formatDate(Date date, Object... pattern) {
        if (date == null) {
            return null;
        }
        String formatDate = null;
        if (pattern != null && pattern.length > 0) {
            formatDate = DateFormatUtils.format(date, pattern[0].toString());
        } else {
            formatDate = DateFormatUtils.format(date, "yyyy-MM-dd");
        }
        return formatDate;
    }

    public static String formatTimestampMs(long timestampMs) {
       return formatDate(new Date(timestampMs),PARSE_PATTERNS[12]);
    }

    /**
     * 得到日期时间字符串，转换格式（yyyy-MM-dd HH:mm:ss）
     */
    public static String formatDateTime(Date date) {
        return formatDate(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 得到当前时间字符串 格式（HH:mm:ss）
     */
    public static String getTime() {
        return formatDate(new Date(), "HH:mm:ss");
    }

    /**
     * 得到当前日期和时间字符串 格式（yyyy-MM-dd HH:mm:ss）
     */
    public static String getDateTime() {
        return formatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 数仓相关日期参数
     * @return
     */
    public static String getBizDate(){
        return formatDate(addDay(new Date(),-1), "yyyyMMdd");
    }

    public static String getBizDate2(){
        return formatDate(addDay(new Date(),-1), "yyyy-MM-dd");
    }

    public static String getBizDateFormat(String format){
        return formatDate(addDay(new Date(),-1), format);
    }

    public static long getBizDateTimestamp(){
        String bizStr = getBizDate2();
        Date bizdate = null;
        try {
            bizdate = parseDate(bizStr, PARSE_PATTERNS[0]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return bizdate.getTime();
    }

    public static String getHiveDefaultClearDate(){
        return formatDate(addDay(new Date(),-2), "yyyyMMdd");
    }

    /**
     * 得到当前年份字符串 格式（yyyy）
     */
    public static String getYear() {
        return formatDate(new Date(), "yyyy");
    }

    /**
     * 得到当前月份字符串 格式（MM）
     */
    public static String getMonth() {
        return formatDate(new Date(), "MM");
    }

    /**
     * 得到当天字符串 格式（dd）
     */
    public static String getDay() {
        return formatDate(new Date(), "dd");
    }

    /**
     * 得到当天字符串 格式（HH）
     */
    public static String getHour() {
        return formatDate(new Date(), "HH");
    }

    /**
     * 得到当前星期字符串 格式（E）星期几
     */
    public static String getWeek() {
        return formatDate(new Date(), "E");
    }

    /**
     * 日期型字符串转化为日期 格式
     * { "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm",
     * "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm",
     * "yyyy.MM.dd", "yyyy.MM.dd HH:mm:ss", "yyyy.MM.dd HH:mm" }
     */
    public static Date parseDate(Object str) {
        if (str == null) {
            return null;
        }
        try {
            return parseDate(str.toString(), PARSE_PATTERNS);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 获取过去的天数
     *
     * @param date
     * @return
     */
    public static long pastDays(Date date) {
        long t = System.currentTimeMillis() - date.getTime();
        return t / (24 * 60 * 60 * 1000);
    }

    /**
     * 获取过去的小时
     *
     * @param date
     * @return
     */
    public static long pastHour(Date date) {
        long t = System.currentTimeMillis() - date.getTime();
        return t / (60 * 60 * 1000);
    }

    /**
     * 获取过去的分钟
     *
     * @param date
     * @return
     */
    public static long pastMinutes(Date date) {
        long t = System.currentTimeMillis() - date.getTime();
        return t / (60 * 1000);
    }

    /**
     * 转换为时间（天,时:分:秒.毫秒）
     *
     * @param timeMillis
     * @return
     */
    public static String formatDateTime(long timeMillis) {
        long day = timeMillis / (24 * 60 * 60 * 1000);
        long hour = (timeMillis / (60 * 60 * 1000) - day * 24);
        long min = ((timeMillis / (60 * 1000)) - day * 24 * 60 - hour * 60);
        long s = (timeMillis / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
        long sss = (timeMillis - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000 - min * 60 * 1000 - s * 1000);
        return (day > 0 ? day + "," : "") + hour + ":" + min + ":" + s + "." + sss;
    }

    /**
     * 获取两个日期之间的天数
     *
     * @param before
     * @param after
     * @return
     */
    public static double getDistanceOfTwoDate(Date before, Date after) {
        long beforeTime = before.getTime();
        long afterTime = after.getTime();
        return (afterTime - beforeTime) / (1000 * 60 * 60 * 24);
    }

    public static String getFirstDayOfMonth() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        //获取当前月第一天：
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
        String first = format.format(c.getTime());
        return first;
    }

    /**
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {

        System.out.println(getHour());

        System.out.println(String.format("%02d",1));


        String startStr=DateUtils.getBizDate2()+" "+String.format("%02d",6)+":00:00";
        String endStr=DateUtils.getBizDate2()+" "+String.format("%02d",11)+":59:59";

        System.out.println(DateUtils.parseDate(startStr,PARSE_PATTERNS[1]));
        System.out.println(endStr);


    }

}
