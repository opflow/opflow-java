package com.devebot.opflow.supports;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;

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
        List<String> strs = new ArrayList<>();
        if (period.getYears() > 0) {
            strs.add(period.getYears() + " year(s)");
            middle = true;
        }
        if (middle || period.getMonths() > 0) {
            strs.add(period.getMonths() + " month(s)");
            middle = true;
        }
        if (middle || period.getDays() > 0) {
            strs.add(period.getDays() + " day(s)");
        }
        if (middle || period.getHours() > 0) {
            strs.add(period.getHours() + " hour(s)");
        }
        if (middle || period.getMinutes() > 0) {
            strs.add(period.getMinutes() + " minute(s)");
        }
        if (middle || period.getSeconds() > 0) {
            strs.add(period.getSeconds() + " second(s)");
        }
        if (strs.isEmpty()) {
            return "0";
        }
        return String.join(", ", strs);
    }
}
