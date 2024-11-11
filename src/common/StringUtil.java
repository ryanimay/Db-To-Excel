package common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
    public static String decodeBig5Word(String input)
    {
        Pattern pattern = Pattern.compile("&#(\\d+);");
        Matcher matcher = pattern.matcher(input);

        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            int codePoint = Integer.parseInt(matcher.group(1));

            String decodedChar = Character.toString((char) codePoint);

            matcher.appendReplacement(result, decodedChar);
        }

        matcher.appendTail(result);

        return result.toString();
    }
}
