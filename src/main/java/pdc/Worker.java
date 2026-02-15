package pdc;

import java.io.*;
import java.net.*;

/**
 * Worker node using JSON protocol
 */
public class Worker {

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;
    private String workerId;
    private String masterHost;
    private int masterPort;
    private String studentId;
    private volatile boolean running = false;

    public Worker() {
        this.workerId = System.getenv("WORKER_ID");
        if (workerId == null) workerId = "worker-" + System.currentTimeMillis();
        this.masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null) masterHost = "localhost";
        String portStr = System.getenv("MASTER_PORT");
        this.masterPort = portStr != null ? Integer.parseInt(portStr) : 5000;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) studentId = "DEFAULT_STUDENT";
    }

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) studentId = "DEFAULT_STUDENT";
    }

    public void connect() {
        try {
            socket = new Socket(masterHost, masterPort);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true);
            
            System.out.println("[Worker " + workerId + "] Connected to " + masterHost + ":" + masterPort);
            running = true;
            registerWithMaster();
        } catch (IOException e) {
            System.err.println("[Worker] Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void registerWithMaster() {
        try {
            Message msg = new Message();
            msg.messageType = "REGISTER_WORKER";
            msg.studentId = workerId;
            msg.payloadStr = workerId;
            out.println(msg.toJson());
            System.out.println("[Worker " + workerId + "] Registration sent");
        } catch (Exception e) {
            System.err.println("[Worker] Registration error: " + e.getMessage());
        }
    }

    public void execute() {
        Thread listener = new Thread(() -> {
            try {
                String line;
                while (running && (line = in.readLine()) != null) {
                    try {
                        if (line.trim().isEmpty()) continue;
                        
                        Message msg = Message.parse(line);
                        String type = msg.messageType != null ? msg.messageType : msg.type;
                        System.out.println("[Worker " + workerId + "] Received " + type);

                        if ("RPC_REQUEST".equals(type)) {
                            handleRpcRequest(msg);
                        } else if ("HEARTBEAT".equals(type)) {
                            Message ack = new Message();
                            ack.messageType = "HEARTBEAT_ACK";
                            ack.studentId = studentId;
                            out.println(ack.toJson());
                        }
                    } catch (Exception e) {
                        System.err.println("[Worker " + workerId + "] Process error: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("[Worker " + workerId + "] Disconnected");
            }
            running = false;
        });
        listener.setDaemon(true);
        listener.start();
    }

    private void handleRpcRequest(Message msg) {
        try {
            String payload = msg.payloadStr != null ? msg.payloadStr : "";
            System.out.println("[Worker " + workerId + "] Processing: " + payload);
            
            Message response = new Message();
            response.messageType = "TASK_COMPLETE";
            response.studentId = studentId;
            response.payloadStr = payload + ";processed";
            out.println(response.toJson());
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] RPC error: " + e.getMessage());
        }
    }

    public void joinCluster(String host, int port) {
        this.masterHost = host;
        this.masterPort = port;
        connect();
    }

    public static void main(String[] args) {
        Worker w = new Worker();
        w.connect();
        w.execute();
        try { Thread.currentThread().join(); } catch (InterruptedException e) {}
    }
}
