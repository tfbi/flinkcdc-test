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
     * date1 - date2????????????
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
     * ??????????????????????????? ?????????yyyy-MM-dd???
     */
    public static String getDate() {
        return getDate("yyyy-MM-dd");
    }

    /**
     * ??????????????????????????? ?????????yyyy-MM-dd??? pattern????????????"yyyy-MM-dd" "HH:mm:ss" "E"
     */
    public static String getDate(String pattern) {
        return DateFormatUtils.format(new Date(), pattern);
    }

    /**
     * ????????????????????? ???????????????yyyy-MM-dd??? pattern????????????"yyyy-MM-dd" "HH:mm:ss" "E"
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
     * ?????????????????????????????????????????????yyyy-MM-dd HH:mm:ss???
     */
    public static String formatDateTime(Date date) {
        return formatDate(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * ??????????????????????????? ?????????HH:mm:ss???
     */
    public static String getTime() {
        return formatDate(new Date(), "HH:mm:ss");
    }

    /**
     * ???????????????????????????????????? ?????????yyyy-MM-dd HH:mm:ss???
     */
    public static String getDateTime() {
        return formatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * ????????????????????????
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
     * ??????????????????????????? ?????????yyyy???
     */
    public static String getYear() {
        return formatDate(new Date(), "yyyy");
    }

    /**
     * ??????????????????????????? ?????????MM???
     */
    public static String getMonth() {
        return formatDate(new Date(), "MM");
    }

    /**
     * ????????????????????? ?????????dd???
     */
    public static String getDay() {
        return formatDate(new Date(), "dd");
    }

    /**
     * ????????????????????? ?????????HH???
     */
    public static String getHour() {
        return formatDate(new Date(), "HH");
    }

    /**
     * ??????????????????????????? ?????????E????????????
     */
    public static String getWeek() {
        return formatDate(new Date(), "E");
    }

    /**
     * ????????????????????????????????? ??????
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
     * ?????????????????????
     *
     * @param date
     * @return
     */
    public static long pastDays(Date date) {
        long t = System.currentTimeMillis() - date.getTime();
        return t / (24 * 60 * 60 * 1000);
    }

    /**
     * ?????????????????????
     *
     * @param date
     * @return
     */
    public static long pastHour(Date date) {
        long t = System.currentTimeMillis() - date.getTime();
        return t / (60 * 60 * 1000);
    }

    /**
     * ?????????????????????
     *
     * @param date
     * @return
     */
    public static long pastMinutes(Date date) {
        long t = System.currentTimeMillis() - date.getTime();
        return t / (60 * 1000);
    }

    /**
     * ?????????????????????,???:???:???.?????????
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
     * ?????????????????????????????????
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
        //???????????????????????????
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);//?????????1???,?????????????????????????????????
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
