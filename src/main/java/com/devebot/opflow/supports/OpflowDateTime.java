package com.devebot.opflow.supports;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 *
 * @author drupalex
 */
public class OpflowDateTime {
    
    public static String printElapsedTime(Date startDate, Date endDate) {
        Interval interval = new Interval(startDate.getTime(), endDate.getTime());
        Period period = interval.toPeriod();
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
        return String.join(", ", strs);
    }
}
