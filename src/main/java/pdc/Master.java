package pdc;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Master coordinator using JSON protocol over sockets
 */
public class Master {

    private int port;
    private ServerSocket serverSocket;
    private ConcurrentHashMap<Integer, Client> clients = new ConcurrentHashMap<>();
    private int clientIdCounter = 0;
    private ExecutorService threadPool;
    private String studentId;
    private volatile boolean running = false;

    static class Client {
        int id;
        Socket socket;
        PrintWriter out;
        Client(int id, Socket socket) { this.id = id; this.socket = socket; }
    }

    public Master() throws IOException {
        this(5000);
    }

    public Master(int port) throws IOException {
        this.port = port;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) studentId = "DEFAULT_STUDENT";
        this.threadPool = Executors.newFixedThreadPool(10);
        this.serverSocket = new ServerSocket(port);
        this.running = true;
        System.out.println("[Master] Initialized on port " + port);
    }

    public void start() {
        System.out.println("[Master] Starting on port " + port);
        threadPool.execute(this::acceptClients);
    }

    private synchronized int nextClientId() {
        return clientIdCounter++;
    }

    private void acceptClients() {
        try {
            while (running) {
                Socket socket = serverSocket.accept();
                int id = nextClientId();
                Client client = new Client(id, socket);
                clients.put(id, client);
                System.out.println("[Master] Client " + id + " connected");
                threadPool.execute(() -> handleClient(client));
            }
        } catch (IOException e) {
            if (running) e.printStackTrace();
        }
    }

    private void handleClient(Client client) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(client.socket.getInputStream(), "UTF-8"));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(client.socket.getOutputStream(), "UTF-8"), true)) {

            client.out = out;
            String line;
            
            while ((line = in.readLine()) != null) {
                try {
                    if (line.trim().isEmpty()) continue;
                    
                    Message msg = Message.parse(line);
                    String type = msg.messageType != null ? msg.messageType : msg.type;
                    System.out.println("[Master] Received " + type);

                    if ("RPC_REQUEST".equals(type)) {
                        // Echo response
                        Message resp = new Message();
                        resp.messageType = "TASK_COMPLETE";
                        resp.studentId = studentId;
                        resp.payloadStr = (msg.payloadStr != null ? msg.payloadStr : "") + ";success";
                        out.println(resp.toJson());
                    }
                } catch (Exception e) {
                    System.err.println("[Master] Error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[Master] Client disconnected");
        } finally {
            clients.remove(client.id);
        }
    }

    public void listen(int p) throws IOException {
        this.port = p;
        this.serverSocket = new ServerSocket(p);
        start();
    }

    public void reconcileState() {}
    public Object coordinate(String op, int[][] matrix, int numWorkers) { return null; }
    public void shutdown() { running = false; threadPool.shutdown(); }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getenv("MASTER_PORT") != null ? System.getenv("MASTER_PORT") : "5000");
        Master m = new Master(port);
        m.start();
        try { Thread.currentThread().join(); } catch (InterruptedException e) {}
    }
}
