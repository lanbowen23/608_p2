package sql608;

import java.io.BufferedReader;
import java.io.InputStreamReader;

class Interface {
    void start(){
        try {
            System.out.println();
            System.out.println("To execute file, please enter: file absolute_path_to_file\n");
            System.out.println("e.g. file D:\\t1.txt\n");
            Executor virtualSQL = new Executor();

            InputStreamReader inputStreamReader = new InputStreamReader(System.in);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String readString = bufferedReader.readLine();

            virtualSQL.exec(readString);
            do {
                readString = bufferedReader.readLine();
                if (readString != null) {
                    virtualSQL.exec(readString);
                }
            }
            while (readString != null);
        } catch (Exception e) {
            System.out.println("IO error, please check your input");
        }

    }
}
