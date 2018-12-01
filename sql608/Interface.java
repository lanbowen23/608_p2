package sql608;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Interface {
    public void start(){
        try {
            System.out.println();
            System.out.println("TinySQL:");
            System.out.println("To execute file, please enter: source absolute_path_to_file");
            System.out.println();
            System.out.println("Please enter your TinySQL command:");
            Physical pQ = new Physical();

            InputStreamReader inputStreamReader = new InputStreamReader(System.in);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String readString = bufferedReader.readLine();

            pQ.exec(readString);
            do {
                readString = bufferedReader.readLine();
                if (readString != null) {
                    pQ.exec(readString);
                }
            }
            while (readString != null);
        } catch (Exception e) {
            System.out.println("IO error, please check your input");
        }

    }
}
