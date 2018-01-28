package io.github.zuston.test;

/**
 * Created by zuston on 2018/1/24.
 */
public class CheckTest {
    public static void main(String[] args) {
        String line1 = "90000188713356#90000145547719#90000167659974#90000162563353#90000138096763#85000074829048#800006932\n" +
                "96799#85000074829002#90000163076075#90000120417274#60000000293297#30000065892000#90000142479226#90000151661908#90000165506403#9000\n" +
                "0194426234#90000173482402#90000148745316#90000155991272#90000164424317#90000164464924#90000140129535#90000176740724#90000192933005\n" +
                "#85000073354210#90000180329581#90000139939990#90000173179867#90000140103060#81111304832405#90000170609993#90000181306525#900001704\n" +
                "14292#90000167997926#90000157657868#30000054733889#90000088792662#90000189435755#90000175061370#90000172515835#90000165434967#9000\n" +
                "0180773326#90000187799859#90000188557866#81111304815324#90000143188746#90000181306525#30000057008569#90000183083854#90000165984740\n" +
                "#90000194161978#90000193841534#90000168914495#90000167933439#90000165075224#90000185668867#90000130405866#90000176518800#900001807\n" +
                "51769#90000147206885#90000159452560#90000177876138#30000063806126#90000180649207#90000184682919#90000190708437#90000143558344#3000\n" +
                "0055952813#90000168836703#90000165230474#90000133867705#90000193677869#30000050448028#90000192361813#90000183155712#90000143740888\n" +
                "#90000189206347#85000073579734#90000188080150#81111303793165#90000188580326#90000186307700#90000191546410#85000072340601#900001635\n" +
                "65565#90000162409011#90000185055385#90000188720805#90000163353854#90000184048576#90000162382858#60000000310249#90000143281614#9000\n" +
                "0169977240#90000098779388#85000074829025#90000194788176#90000191558505#90000184431875#90000102640610#90000173482402";

        String line2 = "90000176674153#90000177675827#30000046231473#30000065731802#30000056906394#90000179838170#900001857\n" +
                "31074#90000187696516#90000182400304#90000180738068#81111304795651#90000175083092#90000165485326#30000057364914#90000165276817#9000\n" +
                "0175076451#90000189164004#30000058090438#30000012246252#30000042670092#90000165816718#90000183350674#30000053863681#30000062587546\n" +
                "#90000148325235#90000175310353#90000187334171#90000180729605#90000179261228#90000189028359#30000057261685#90000183641316#900001900\n" +
                "69475#90000169382845#90000186842514#30000055168985#90000177051617#80000187486297#30000059207183#30000065900028#30000058444587#9000\n" +
                "0190161218#90000193305140#30000050438415#30000042670280#30000058277107#90000176417221#90000193876564#90000160461037#85000058235571\n" +
                "#85000052354260#85000055009811#90000178235369#90000192723943#90000192642690#90000176417221#90000192822481#90000192832500#900001851\n" +
                "11828#30000063442100#90000189164004#90000182438081#90000173830284#30000059241645#90000179162508#30000054453062#90000194316701#9000\n" +
                "0189590012#90000183401900#30000039997953#30000041449528#90000171076085#87000213023033#90000187832989#85000052354260#30000039368311\n" +
                "#90000193621398#90000190945826#30000059459791#90000193866829#90000190161218#90000194116461#90000188072235#30000041449528#900001927\n" +
                "23943#90000187769882#30000042667690#90000178532434#90000178235369#30000063391178#90000182099767#90000184975156#81111303859125#3000\n" +
                "0063840207#30000065900028#90000181982095#85000055166080#90000181616703#30000066002037#90000181322123#90000188914413";

    }

    public static void check(String line1, String line2){
        String [] l1 = line1.split("#");
        String [] l2 = line2.split("#");
        for (String a : l1){
            for (String b : l2){
                if (a.equals(b)){
                    System.out.println(a);
                }
            }
        }
    }
}