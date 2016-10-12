import java.io.*;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dell on 2016/10/4.
 */
public class WriteData {
    public static void main(String[] args) throws IOException {
        String[] gender = {"M", "F"};
        String[] occupation = {"dministrator",
                "artist",
                "doctor",
                "educator",
                "engineer",
                "entertainment",
                "executive",
                "healthcare",
                "homemaker",
                "lawyer",
                "librarian",
                "marketing",
                "none",
                "other",
                "programmer",
                "retired",
                "salesman",
                "scientist",
                "student",
                "technician",
                "writer"};
        BufferedWriter fos = new BufferedWriter(new FileWriter("userdata.txt"));
        Random rand = new Random();
        for(int i =0;i<100;i++){
            fos.write(String.format("(%03d,%02d,'%s','%s',%05d),", rand.nextInt(9900)+100,rand.nextInt(100),gender[rand.nextInt(2)],occupation[rand.nextInt(21)], rand.nextInt(990000)+10000));
        }


        fos.close();
    }
    private static String randomString() {
        char[] str = "qwertyuiopasdfghjklzxcvbnm1234567890".toCharArray();
        Random rand = new Random();
//        int num  = rand.nextInt(20);
//        if (num < 5) num = 5;
        int num = 30;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < num; i++) {
            sb.append(str[rand.nextInt(36)]);
        }
        return sb.toString();
    }

    private static void second() throws IOException {
        BufferedWriter fos = new BufferedWriter(new FileWriter("logdata.txt"));
        Random rand = new Random();
        for(int i =0;i<10000;i++){
            fos.write(rand.nextInt(31962538) + "\t" + randomString() + "\n");
        }
        fos.close();
    }
    private static void first() throws IOException {
        String[] cities = {"London", "NewYork", "Beijing", "Nanjing"};
        Random rand = new Random();
        BufferedWriter fos = new BufferedWriter(new FileWriter("weatherdata.txt"));
        for(int i =0;i<10000;i++){
            fos.write(cities[i%4] + "\t" + rand.nextInt(37) + "\t" + rand.nextInt(10) + "\n");
        }
        fos.close();
    }
}
