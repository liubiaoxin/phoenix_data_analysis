/*
package net.phoenix.bigdata.common.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.DateTimeZone;

import java.util.Date;
import java.util.TimeZone;

public class DateUtils {


    public final static String  timeZone;    // 当前时区
    private static DateTimeZone dateTimeZone;

    static {
        TimeZone localTimeZone = TimeZone.getDefault();
        int rawOffset = localTimeZone.getRawOffset();
        String symbol = "+";
        if (rawOffset < 0) {
            symbol = "-";
        }
        rawOffset = Math.abs(rawOffset);
        int offsetHour = rawOffset / 3600000;
        int offsetMinute = rawOffset % 3600000 / 60000;
        String hour = String.format("%1$02d", offsetHour);
        String minute = String.format("%1$02d", offsetMinute);
        timeZone = symbol + hour + ":" + minute;
        dateTimeZone = DateTimeZone.forID(timeZone);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT" + timeZone));
    }

    */
/**
     * 通用日期时间字符解析
     *
     * @param datetimeStr 日期时间字符串
     * @return Date
     *//*

    public static Date parseDate(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        datetimeStr = datetimeStr.trim();
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(" ", "T");
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        }

        DateTime dateTime = new DateTime(datetimeStr, dateTimeZone);

        return dateTime.toDate();
    }

    public static void main(String[] args) {

        Date date = parseDate("2020.04.10 15:36:45");

        System.out.println(date);


    }

}
*/
