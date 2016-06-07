package fourth;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnHelper {

    public static final Pattern SPACE = Pattern.compile("\\s+(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    private static final Pattern PAGE_IP = Pattern.compile("/id.*HTTP");

    public static String getIp(String string) {
        return SPACE.split(string)[0];
    }
    public static String getPageIp(String string) {
        String requestDescription = SPACE.split(string)[5];
        Matcher matcher = PAGE_IP.matcher(requestDescription);
        if (matcher.find()) return SPACE.split(matcher.group(0).replace("/id", ""))[0];
        return null;
    }

    public static boolean isUserPage(String string) {
        return SPACE.split(string)[5].contains("/id");
    }

}
