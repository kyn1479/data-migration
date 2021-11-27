package com.kkk.data.migration.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
public class DateUtil {

    public final static String pattern1 = "yyyyMMdd";
    private final static String pattern2 = "yyyy-MM-dd";
    public final static String pattern3 = "yyyy-MM-dd HH:mm:ss";
    public final static String pattern4 = "yyyyMMddHHmmss";
    private final static String pattern5 = "yyyyMMddHHmmssSSS";

    protected static final ConcurrentMap<String, DateFormat> FORMATER_CACHE = new ConcurrentHashMap<String, DateFormat>();

    /**
     * 获取当前时间
     *
     * @return
     */
    public static Date getCurrentDateTime() {
        return DateTime.now().toDate();
    }

    /**
     * 获取当前日期
     *
     * @return
     */
    public static java.sql.Date getCurrentDate() {
        return new java.sql.Date(getCurrentDateTime().getTime());
    }

    /**
     * 获取当前日期/时间
     *
     * @param pattern
     * @return
     */
    public static String getCurrentDateStr(String pattern) {
        return DateTime.now().toString(pattern);
    }


    public static String getNextDateStr(String pattern) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + 1);
        String nextday = format.format(cal.getTime());
        return nextday;
    }

    /**
     * 获取minutes分之后的时间
     * @param minutes
     * @return
     */
    public static String getNextMinuteStr(int minutes) {
        return DateTime.now().plusMinutes(minutes).toString(pattern4);
    }

    /**
     * 获取当前日期(yyyyMMdd)
     *
     * @return
     */
    public static String getCurrentDateStr() {
        return getCurrentDateStr(pattern1);
    }

    /**
     * 获取当前时间(yyyy-MM-dd HH:mm:ss)
     *
     * @return
     */
    public static String getCurrentDateTimeStr() {
        return getCurrentDateStr(pattern3);
    }

    /**
     * 获取当前时间(yyyyMMddHHmmssSSS)
     *
     * @return
     */
    public static String getCurrentDateTimeSSSStr() {
        return getCurrentDateStr(pattern5);
    }

    /**
     * 日期/时间格式化
     *
     * @param date
     * @param pattren
     * @return
     */
    public static String format(Date date, String pattren) {
        return new DateTime(date).toString(pattren);
    }

    /**
     * 时间解析
     *
     * @param dateTime
     * @param pattren
     * @return
     */
    public static Date parseDateTime(String dateTime, String pattren) {
        return DateTimeFormat.forPattern(pattren).parseDateTime(dateTime).toDate();
    }

    /**
     * 将不同日期格式的字符串，转为date对象
     * @param dateStr
     * @param pattren
     * @return
     * @throws ParseException
     */
    public static Date changeFormat2Date(String dateStr, String pattren) {
        Date date = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattren, Locale.ENGLISH);
            date = null;
            date = sdf.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 日期解析
     *
     * @param date
     * @param pattren
     * @return
     */
    public static java.sql.Date parseDate(String date, String pattren) {
        return new java.sql.Date(DateTimeFormat.forPattern(pattren).parseDateTime(date).toDate().getTime());
    }

    /**
     * 获取前days日的日期
     *
     * @param days
     * @return
     */
    public static java.sql.Date getLastDateByDays(int days) {
        return new java.sql.Date(DateTime.now().minusDays(days).withMillisOfDay(0).toDate().getTime());
    }
    /**
     * 获取后years
     *
     * @param years
     * @return
     */
    public static Date getAfterDateByYear(int years) {
        return DateTime.now().plusYears(years).toDate();
    }
    public static Date addDays(Date date, int days) {
        return DateUtils.addDays(date, days);
    }

    public static Date addYears(Date date, int years) {
        return addDays(DateUtils.addYears(date, years), -1);
    }

    /**
     * 获取前minutes分的时间
     *
     * @param minutes
     * @return
     */
    public static Date getLastDateTimeByMinutes(int minutes) {
        return DateTime.now().minusMinutes(minutes).toDate();
    }

    /**
     * 获取minutes分之后的时间
     * @param minutes
     * @return
     */
    public static Date getNextDateTimeByMinutes(int minutes) {
        return DateTime.now().plusMinutes(minutes).toDate();
    }

    /**
     * 校验字符串是否符合日期格式
     * （1）将dateTimeStr按指定格式（pattern）进行转换，如果异常说明其格式不对
     * （2）将步骤（1）的日期再次按指定格式（pattern）转换为日期字符串，并与原字符串进行比较，如果一致表示校验成功
     *
     * @param dateTimeStr 待校验的日期字符串
     * @param pattern     日期格式，例如yyyyMMdd
     * @return result 1：符合格式；0：转换前后不一致；-1：表示异常（主要是格式异常）
     */
    public static int checkDateFormatAndValidate(String dateTimeStr, String pattern) {
        int result = 0;

        try {
            DateTime dateTime = DateTimeFormat.forPattern(pattern).parseDateTime(dateTimeStr);

            String convertedDateTimeStr = dateTime.toString(pattern);
            if (StringUtils.equals(dateTimeStr, convertedDateTimeStr)) {
                result = 1;
            }
        } catch (Throwable throwable) {
            result = -1;
        }

        return result;
    }

    /**
     * 获取时间毫秒数
     * @param date
     * @return
     */
    public static String getTimeMillis(Date date){
        return String.valueOf(date.getTime());
    }

    /**
     * 获取date对应的星期数
     *
     * @param date
     * @return
     */
    public static int getWeekOfDayInt(Date date){
        Calendar calendar = Calendar.getInstance();
        if(date != null){
//            calendar.add(Calendar.DATE, 3);
            calendar.setTime(date);
        }
        int w = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0){
            w = 0;
        }
        return w;
    }


    public static Date getAppointedDateByHour(int hour) {
        Calendar time = Calendar.getInstance();
        time.set(Calendar.HOUR_OF_DAY, hour);
        time.set(Calendar.MINUTE, 00);
        time.set(Calendar.SECOND, 00);
        time.set(Calendar.MILLISECOND, 999);
        return time.getTime();
    }

    /**
     * 获取当前时间戳13位,精确到毫秒
     * @return long
     */
    public static long getCurrentDateTimeStamp() {
        return DateTime.now().toDate().getTime();
    }

    //获取2日期之间的日期
    public static List<Date> getBetweenDates(Date start, Date end) {
        List<Date> result = new ArrayList<Date>();
        Calendar tempStart = Calendar.getInstance();
        tempStart.setTime(start);
        tempStart.add(Calendar.DAY_OF_YEAR, 1);
        if(!result.contains(start)){
            result.add(start);
        }
        if(!result.contains(end)){
            result.add(end);
        }

        Calendar tempEnd = Calendar.getInstance();
        tempEnd.setTime(end);
        while (tempStart.before(tempEnd)) {
            if(!result.contains(tempStart.getTime())){
                result.add(tempStart.getTime());
            }
            tempStart.add(Calendar.DAY_OF_YEAR, 1);
        }
        return result;
    }
    /**
     * 获取amout之前的日期
     * @param date
     * @return
     * @throws ParseException
     */
    public static Date getBeforeDay(Date date,int amout) {
        if (date != null) {
            Calendar cad = Calendar.getInstance();
            cad.setTime(date);
            cad.add(Calendar.DATE, -amout);
            String dateStr= DateUtil.dtSimpleFormat(cad.getTime());
            String dateTimeStr=  dateStr+" 00:00:00";
            return changeFormat2Date(dateTimeStr,pattern3);
        }
        return null;
    }

    /**
     * yyyy-MM-dd
     *
     * @param date
     * @return
     */
    public static final String dtSimpleFormat(Date date) {
        if (date == null) {
            return "";
        }
        return getFormat(pattern1).format(date);
    }
    /**
     * 获取格式
     *
     * @param format
     * @return
     */
    public static final DateFormat getFormat(String format) {
        DateFormat dateFormat = FORMATER_CACHE.get(format);
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat(format);
            FORMATER_CACHE.put(format, dateFormat);
        }
        return dateFormat;
    }

    public static String getYestodayStart(){
        Date dNow = new Date(); //当前时间
        Date dBefore = new Date();
        Calendar calendar = Calendar.getInstance(); //得到日历
        calendar.setTime(dNow);//把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, -1); //设置为前一天
        dBefore = calendar.getTime(); //得到前一天的时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式
        String defaultStartDate = sdf.format(dBefore); //格式化前一天
        defaultStartDate = defaultStartDate+" 00:00:00";
        return defaultStartDate;
    }
    public static String getYestodayEnd(){
        Date dNow = new Date(); //当前时间
        Date dBefore = new Date();
        Calendar calendar = Calendar.getInstance(); //得到日历
        calendar.setTime(dNow);//把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, -1); //设置为前一天
        dBefore = calendar.getTime(); //得到前一天的时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式
        String defaultStartDate = sdf.format(dBefore); //格式化前一天
        String defaultEndDate = defaultStartDate+" 23:59:59";
        return defaultEndDate;
    }
    public static String getAppointedDayStart(int count){
        Date dNow = new Date(); //当前时间
        Date dBefore = new Date();
        Calendar calendar = Calendar.getInstance(); //得到日历
        calendar.setTime(dNow);//把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, count);
        dBefore = calendar.getTime(); //得到前一天的时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式
        String defaultStartDate = sdf.format(dBefore); //格式化前一天
        defaultStartDate = defaultStartDate+" 00:00:00";
        return defaultStartDate;
    }

    public static String getAppointedDay(int count){
        Date dNow = new Date(); //当前时间
        Date dBefore = new Date();
        Calendar calendar = Calendar.getInstance(); //得到日历
        calendar.setTime(dNow);//把当前时间赋给日历
        calendar.add(Calendar.DAY_OF_MONTH, count);
        dBefore = calendar.getTime(); //得到前一天的时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //设置时间格式
        String defaultStartDate = sdf.format(dBefore); //格式化前一天
        return defaultStartDate;
    }

    /**
     * 日期格式转换 yyyyMMdd-->yyyy-MM-dd
     * @param str
     * @return
     */
    public static String dateConvertion(String str) {
        Date parse = null;
        String dateString = "";
        try {
            parse = new SimpleDateFormat("yyyyMMdd").parse(str);
            dateString = new SimpleDateFormat("yyyy-MM-dd").format(parse);
        } catch (ParseException e) {
            dateString = null;
        }
        return dateString;
    }

    public String formateDate(String datetime){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(pattern3);
        LocalDateTime ldt = LocalDateTime.parse(datetime,dtf);
        DateTimeFormatter fa = DateTimeFormatter.ofPattern(pattern4);
        return ldt.format(fa);
    }
}
