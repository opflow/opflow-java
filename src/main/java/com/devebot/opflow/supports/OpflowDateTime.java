package com.devebot.opflow.supports;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.joda.time.nostro.Interval;
import org.joda.time.nostro.Period;

/**
 *
 * @author drupalex
 */
public class OpflowDateTime {
    
    private static final String ISO8601_TEMPLATE = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat(ISO8601_TEMPLATE);
    
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    public static String toISO8601UTC(Date date) {
        return DATE_FORMAT.format(date);
    }
    
    public static Date fromISO8601UTC(String dateStr) {
        try {
            return DATE_FORMAT.parse(dateStr);
        } catch (ParseException e) {}
        return null;
    }
    
    public static long getCurrentTime() {
        return (new Date()).getTime();
    }
    
    public static String getCurrentTimeString() {
        return toISO8601UTC(new Date());
    }
    
    public static String printElapsedTime(long duration) {
        return printElapsedTime(new Period(duration));
    }
    
    public static String printElapsedTime(Date startDate, Date endDate) {
        Interval interval = new Interval(startDate.getTime(), endDate.getTime());
        return printElapsedTime(interval.toPeriod());
    }
    
    private static String printElapsedTime(Period period) {
        boolean middle = false;
        boolean small = true;
        List<String> strs = new ArrayList<>();
        int years = period.getYears();
        if (years > 0) {
            strs.add(years + " year(s)");
            middle = true;
            small = false;
        }
        int months = period.getMonths();
        if (middle || months > 0) {
            strs.add(months + " month(s)");
            middle = true;
            small = false;
        }
        int days = period.getDays();
        if (middle || days > 0) {
            strs.add(days + " day(s)");
            small = false;
        }
        int hours = period.getHours();
        if (middle || hours > 0) {
            strs.add(hours + " hour(s)");
            small = false;
        }
        int minutes = period.getMinutes();
        if (middle ||  minutes > 0) {
            strs.add(minutes + " minute(s)");
            if (minutes > 1) {
                small = false;
            }
        }
        int seconds = period.getSeconds();
        if (middle || seconds > 0) {
            strs.add(seconds + " second(s)");
        }
        if (small) {
            int millis = period.getMillis();
            if (middle || millis > 0) {
                strs.add(millis + " millisecond(s)");
            }
        }
        if (strs.isEmpty()) {
            return "0";
        }
        return String.join(", ", strs);
    }
}
