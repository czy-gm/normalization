package normal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {

    private static boolean quiet = false;

    public static void quiet() {
        quiet = true;
    }

    public static void info(String format, Object ... args) {
        if (quiet) {
            return;
        }
        System.out.printf(format, args);
    }
}
