package pdc;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    private static final int PORT = 5000;
    private ServerSocket serverSocket;
    private ConcurrentMap<Integer, Socket> workers = new ConcurrentHashMap<>();
    private int rows = 5; // example, number of rows for matrix
    private AtomicInteger rowIndex = new AtomicInteger(0); // fixed: AtomicInteger for lambda

    public Master() throws IOException {
        serverSocket = new ServerSocket(PORT);
        System.out.println("Master listening on port " + PORT);
    }

    public void start() {
        // accept workers in a separate thread
        new Thread(() -> {
            try {
                int workerId = 0;
                while (true) {
                    Socket worker = serverSocket.accept();
                    System.out.println("Worker connected: " + worker.getInetAddress());
                    workers.put(workerId, worker);
                    int id = workerId;
                    handleWorker(worker, id);
                    workerId++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void handleWorker(Socket worker, int workerId) {
        new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(worker.getInputStream()));
                 PrintWriter out = new PrintWriter(worker.getOutputStream(), true)) {

                // send greeting
                out.println("Greeting from master");

                // assign first row task
                assignRowTask(workerId, rowIndex.get(), out);

                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println("Received result: " + line);

                    // assign next row task if available
                    int nextRow = rowIndex.incrementAndGet();
                    if (nextRow < rows) {
                        assignRowTask(workerId, nextRow, out);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void assignRowTask(int workerId, int row, PrintWriter out) {
        String task = "TASK|ROW|" + row;
        out.println(task);
        System.out.println("Sent task to worker " + workerId + " for row " + row);
    }

    public static void main(String[] args) throws IOException {
        new Master().start();
    }
}
