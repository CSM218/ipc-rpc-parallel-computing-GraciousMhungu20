package pdc;

import java.io.*;
import java.net.*;

public class Worker {

    private static final String HOST = "localhost";
    private static final int PORT = 5000;

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    public void connectAndWork() {
        try {
            socket = new Socket(HOST, PORT);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            System.out.println("Connected to master at " + HOST + ":" + PORT);

            // send greeting
            out.println("Greetings sent.");

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("Received task: " + line);

                if (line.startsWith("TASK|ROW|")) {
                    int row = Integer.parseInt(line.split("\\|")[2]);
                    String result = computeRow(row);
                    out.println(result);
                    System.out.println("Result sent: " + result);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String computeRow(int row) {
        // dummy computation: for example, just row*2 and row*3
        int val1 = row * 2;
        int val2 = row * 3;
        return row + "|" + val1 + "," + val2;
    }

    public static void main(String[] args) {
        new Worker().connectAndWork();
    }
}
