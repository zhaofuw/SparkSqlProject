package com.fw.text;

/**
 * Created by Administrator on 2018/4/12.
 */
public class Test {

    static void changeStr(String str){
        str = "ni ma ge bi a";
    }

    static void internTest(){
        StringBuffer stringBuilder = new StringBuffer();
        String aaa = stringBuilder.append("1234").append("abcd").toString();
        String bbb = "1234abcd";
        String ccc = "1234abcd";

        String ddd = new String("1234abcd");
        String eee = new String("1234abcd");

        System.out.println(aaa.intern()=="1234abcd");
        System.out.println(aaa.intern()==bbb);
        System.out.println(bbb.intern()==aaa);
        System.out.println(bbb.intern()==ccc);

        System.out.println(bbb.equals(aaa));
        System.out.println(aaa.equals(bbb));
        System.out.println(bbb==ccc);
        System.out.println(ddd==eee);
    }
    public static void main(String[] args){
        String aaa = "lao zhao shi ge sha bi";
        //changeStr(aaa);
        internTest();
        System.out.println(aaa);
    }
}
