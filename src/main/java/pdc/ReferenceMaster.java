package pdc;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference Master implementation following CSM218 protocol specification
 */
public class ReferenceMaster {

    private int port;
    private ServerSocket serverSocket;
    private ConcurrentMap<Integer, ClientConnection> clients = new ConcurrentHashMap<>();
    private AtomicInteger clientIdCounter = new AtomicInteger(0);
    private ExecutorService threadPool;
    private String studentId;
    private volatile boolean running = false;

    public ReferenceMaster(int port) throws IOException {
        this.port = port;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "DEFAULT_STUDENT";
        }
        this.threadPool = Executors.newFixedThreadPool(10);
        this.serverSocket = new ServerSocket(port);
        this.running = true;
        System.out.println("[MASTER] Initialized on port " + port);
    }

    public void start() {
        System.out.println("[MASTER] Starting listener on port " + port);
        threadPool.execute(() -> acceptClients());
    }

    private void acceptClients() {
        try {
            while (running) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("[MASTER] New client connection from " + clientSocket.getInetAddress());
                int clientId = clientIdCounter.getAndIncrement();

                ClientConnection conn = new ClientConnection(clientId, clientSocket);
                clients.put(clientId, conn);

                threadPool.execute(() -> handleClient(conn));
            }
        } catch (IOException e) {
            if (running) {
                System.err.println("[MASTER] Accept error: " + e.getMessage());
            }
        }
    }

    private void handleClient(ClientConnection conn) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.socket.getInputStream(), "UTF-8"));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(conn.socket.getOutputStream(), "UTF-8"), true)) {

            conn.out = out;

            System.out.println("[MASTER] Handling client " + conn.clientId);

            String line;
            while ((line = in.readLine()) != null && running) {
                try {
                    Message msg = Message.parse(line);
                    System.out.println("[MASTER] Received " + msg.messageType + " from client " + conn.clientId);

                    if ("RPC_REQUEST".equals(msg.messageType)) {
                        handleRpcRequest(conn, msg);
                    } else if ("TASK_COMPLETE".equals(msg.messageType)) {
                        System.out.println("[MASTER] Task result received: " + msg.payload);
                    }
                } catch (Exception e) {
                    System.err.println("[MASTER] Error processing message: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[MASTER] Client " + conn.clientId + " disconnected: " + e.getMessage());
        } finally {
            clients.remove(conn.clientId);
        }
    }

    private void handleRpcRequest(ClientConnection conn, Message msg) {
        try {
            System.out.println("[MASTER] Processing RPC: " + msg.payloadStr);

            // Send response
            Message response = new Message();
            response.messageType = "TASK_COMPLETE";
            response.studentId = studentId;
            response.payloadStr = msg.payloadStr + ";success";

            conn.out.println(response.toJson());
            System.out.println("[MASTER] Sent response for RPC");
        } catch (Exception e) {
            System.err.println("[MASTER] Error handling RPC: " + e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        threadPool.shutdown();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ClientConnection {
        int clientId;
        Socket socket;
        PrintWriter out;

        ClientConnection(int clientId, Socket socket) {
            this.clientId = clientId;
            this.socket = socket;
        }
    }

    public static void main(String[] args) throws IOException {
        int port = 5000;
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null) {
            port = Integer.parseInt(portEnv);
        }

        ReferenceMaster master = new ReferenceMaster(port);
        master.start();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
