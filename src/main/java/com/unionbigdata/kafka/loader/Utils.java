package com.unionbigdata.kafka.loader;

import java.sql.BatchUpdateException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * Created by kali on 2015/8/28.
 */
public class Utils {
    private static final String[] shortDays = new String[]{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    private static final String[] completeDays = new String[]{"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};
    private static final String[] shortMonthes = new String[]{"Jan","Feb","Mar","Apr","May","June","July","Aug","Sept","Oct","Nov","Dec"};
    private static final String[] completeMonthes = new String[]{"January","February","March","April","May","June","July","August","September","October","November","December"};
    /**
     * Format the path string ,the following are the escape sequences supported:
     * <table>
     * <tr><td>%t</td><td>Unix time in milliseconds</td></tr>
     * <tr><td>%a</td><td>locale¡¯s short weekday name (Mon, Tue, ...)</td></tr>
     * <tr><td>%t</td><td>Unix time in milliseconds</td></tr>
     * <tr><td>%A</td><td>locale¡¯s full weekday name (Monday, Tuesday, ...)</td></tr>
     * <tr><td>%b</td><td>locale¡¯s short month name (Jan, Feb, ...)</td></tr>
     * <tr><td>%B</td>locale¡¯s long month name (January, February, ...)</tr>
     * <tr><td>%c</td><td>locale¡¯s date and time (Thu Mar 3 23:05:25 2005)</td></tr>
     * <tr><td>%d</td><td>day of month (01)</td></tr>
     * <tr><td>%e</td><td>day of month without padding (1)</td></tr>
     * <tr><td>%D</td><td>date; same as %m/%d/%y</td></tr>
     * <tr><td>%H</td><td>hour (00..23)</td></tr>
     * <tr><td>%I</td><td>hour (01..12)</td></tr>
     * <tr><td>%j</td><td>day of year (001..366)</td></tr>
     * <tr><td>%k</td><td>hour(0..23)</td></tr>
     * <tr><td>%m</td><td>month(01..12</td></tr>
     * <tr><td>%n</td><td>month without padding(1..12)</td></tr>
     * <tr><td>%M</td><td>minute(00..59)</td></tr>
     * <tr><td>%p</td><td>locale¡¯s equivalent of am or pm</td></tr>
     * <tr><td>%s</td><td>seconds since 1970-01-01 00:00:00 UTC</td></tr>
     * <tr><td>%S</td><td>second (00..59)</td></tr>
     * <tr><td>%y</td><td>last two digits of year (00..99)</td></tr>
     * <tr><td>%Y</td><td>year(2010)</td></tr>
     * <tr><td>%z</td><td>+hhmm numeric timezone (for example, -0400)</td></tr>
     * </table>
     * exp.  path = /flume/events/%y-%m-%d/%H%M/%S
     *
     * @param path
     * @return
     */
    public static String pathFormate(String path) {
        if (path == null){
            throw new NullPointerException("path shuold not be null");
        }
        Date now = new Date();
        StringBuffer buf = new StringBuffer(path.length() * 2);

        final char[] chars = path.toCharArray();

        for (int i = 0; i < chars.length; i++){
            if (chars[i] != '%'){
                buf.append(chars[i]);
            }else {
                if (i != chars.length-1){
                    buf.append(escapeSequencesValue(now,chars[i+1]));
                    i++;
                }
            }
        }
        return buf.toString();
    }

    private static String escapeSequencesValue(Date date,char es) throws IllegalArgumentException {
        switch (es){
            case 't':return "" + date.getTime();
            case 'a':return formatDate("EE",date);
            case 'A':return formatDate("EEEE",date);
            case 'b':return formatDate("MMM",date);
            case 'B':return formatDate("MMMM",date);
            case 'c':return formatDate("EE MMM d hh:mm:ss yyyy",date);
            case 'd':return formatDate("dd",date);
            case 'e':return formatDate("d",date);
            case 'D':return formatDate("MM/dd/yyyy", date);
            case 'H':return formatDate("HH",date);
            case 'I':return formatDate("hh",date);
            case 'j':return formatDate("DDD",date);
            case 'k':return formatDate("H",date);
            case 'm':return formatDate("MM",date);
            case 'n':return formatDate("M",date);
            case 'M':return formatDate("mm",date);
            case 'p':return formatDate("a",date);
            case 's':return "" + date.getTime()/1000;
            case 'S':return formatDate("ss",date);
            case 'y':return formatDate("yy",date);
            case 'Y':return formatDate("yyyy",date);
            case 'z':return formatDate("X",date);
            default: throw new IllegalArgumentException("%" + es + " is not a valid escape sequences");
        }

    }

    private static String formatDate(String pattern,Date date){
        return new SimpleDateFormat(pattern, Locale.US).format(date);
    }

}
