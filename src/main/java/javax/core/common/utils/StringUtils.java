package javax.core.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

    /**
     * 将参数str中的如{0} {1}逐个替换成Object... args中对应的数据。
     * @param str
     * @param args
     * @return
     */
    public static String format(String str, Object... args) {
        String result = str;
        //()圆括号是多个匹配。它把括号内的当做一组来处理，分割线|有或者的含义。如(com|cn|net)就可以限制只能是com或cn或net。
        //另：[]方括号是单个匹配。如[abc]它限制的不是abc连续出现，而是只能是其中一个，这样写那么规则就是找到这个位置时只能是a或是b或是c。分割线|没有或者的含义，就是字符，如[es|ed]被当做五个独立的字符。
        Pattern p = Pattern.compile("\\{(\\d+)\\}");
        Matcher m = p.matcher(str);
        //find()输入字符串的如果部分与模式匹配成功返回true，反之返回false。find()从匹配器区域的开头开始执行，每执行一次之后，下次再执行就会从上次匹配到的字符串的下一个字符开始匹配
        //group是针对模式中的()来说的，group(0)和group()指整个模式匹配到的字符串，group(1) 指模式中第一个括号里的东西，group(2)指模式中第二个括号里的东西。
        //另：matches() 将输入序列整个区域与模式匹配，匹配成功返回true，反之返回false。
        while (m.find()) {
            int index = Integer.parseInt(m.group(1));
            if (index < args.length) {
                result = result.replace(m.group(), ObjectUtils.notNull(args[index],"").toString());
            }
        }
        return result;
    }
}
