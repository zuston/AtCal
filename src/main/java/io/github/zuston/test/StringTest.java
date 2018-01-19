package io.github.zuston.test;

/**
 * Created by zuston on 2018/1/17.
 */
public class StringTest {
    public static void main(String[] args) {
//        String line = "你好师姐#最好是这样hsd##但是有100呢";
//        String [] lineArr = line.split("#");
//        int count = 0;
//        for (int i=0; i<2; i++){
//            count += lineArr[i].length();
//        }
//        count += new String("#").length() * 2;
//        System.out.println(count);
//        StringBuilder builder = new StringBuilder(line);
//        builder.insert(count, "1000");
//        System.out.println(builder);

        String pline = "2977395712#85000094247660#60#15148#9021200#青浦分拨中心#20#上海市#265598#贾永增#2017-11-10 21:05:14##郑州分拨中心#9371201###【上海市】青浦分拨中心已发出,下一站郑州分拨中心##2017-11-11 00:10:32##青浦区#.5##0#郑州市#1#QIP201711112595#1058.0892668955462";
//        pline = StringTool.insertOfPrefix(pline,"#","15265", 11);
//        if (pline.contains("_")) pline = pline.replace("_","#");
//        System.out.println(pline);
        System.out.println(pline.substring(0,pline.lastIndexOf("#")));

//        System.out.println(pline.split("#")[11].equals(""));
    }
}
