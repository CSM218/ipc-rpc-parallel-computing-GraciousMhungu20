package pdc;

import java.io.*;
import java.net.*;

/**
 * Reference Worker implementation following CSM218 protocol specification
 */
public class ReferenceWorker {

    private Socket masterSocket;
    private BufferedReader in;
    private PrintWriter out;
    private String workerId;
    private String masterHost;
    private int masterPort;
    private String studentId;
    private volatile boolean running = false;

    public ReferenceWorker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "DEFAULT_STUDENT";
        }
    }

    public ReferenceWorker() {
        this.workerId = System.getenv("WORKER_ID");
        if (workerId == null) {
            workerId = "worker-" + System.currentTimeMillis();
        }
        this.masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null) {
            masterHost = "localhost";
        }

        String portStr = System.getenv("MASTER_PORT");
        this.masterPort = portStr != null ? Integer.parseInt(portStr) : 5000;

        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "DEFAULT_STUDENT";
        }
    }

    public void connect() {
        try {
            masterSocket = new Socket(masterHost, masterPort);
            in = new BufferedReader(new InputStreamReader(masterSocket.getInputStream(), "UTF-8"));
            out = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream(), "UTF-8"), true);

            System.out.println("[WORKER " + workerId + "] Connected to master at " + masterHost + ":" + masterPort);

            running = true;
            registerWithMaster();

        } catch (IOException e) {
            System.err.println("[WORKER " + workerId + "] Failed to connect: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void registerWithMaster() {
        try {
            Message regMsg = new Message();
            regMsg.messageType = "REGISTER_WORKER";
            regMsg.studentId = studentId;
            regMsg.payloadStr = workerId;

            out.println(regMsg.toJson());
            System.out.println("[WORKER " + workerId + "] Registration message sent");
        } catch (Exception e) {
            System.err.println("[WORKER " + workerId + "] Registration error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void execute() {
        new Thread(() -> {
            try {
                String line;
                while (running && (line = in.readLine()) != null) {
                    try {
                        Message msg = Message.parse(line);
                        System.out.println("[WORKER " + workerId + "] Received " + msg.messageType);

                        if ("RPC_REQUEST".equals(msg.messageType)) {
                            handleRpcRequest(msg);
                        } else if ("HEARTBEAT".equals(msg.messageType)) {
                            handleHeartbeat();
                        }
                    } catch (Exception e) {
                        System.err.println("[WORKER " + workerId + "] Error processing message: " + e.getMessage());
                    }
                }
                System.out.println("[WORKER " + workerId + "] Connection closed");
                running = false;
            } catch (IOException e) {
                System.err.println("[WORKER " + workerId + "] I/O error: " + e.getMessage());
            }
        }).start();
    }

    private void handleRpcRequest(Message msg) {
        try {
            String payload = msg.payloadStr;
            System.out.println("[WORKER " + workerId + "] Processing task: " + payload);

            // Parse task
            Object result = null;
            if (payload != null && payload.contains("row:")) {
                // Process matrix row task
                String[] parts = payload.split(":");
                if (parts.length >= 2) {
                    int row = Integer.parseInt(parts[1]);
                    if (parts.length >= 3) {
                        // Parse row values and compute
                        String[] values = parts[2].split(",");
                        int[] rowVals = new int[values.length];
                        for (int i = 0; i < values.length; i++) {
                            rowVals[i] = Integer.parseInt(values[i].trim());
                        }
                        // Simple computation: multiply each by (index+1)
                        int[] processed = new int[rowVals.length];
                        for (int i = 0; i < rowVals.length; i++) {
                            processed[i] = rowVals[i] * (i + 1);
                        }
                        // Format result
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < processed.length; i++) {
                            if (i > 0) sb.append(",");
                            sb.append(processed[i]);
                        }
                        result = sb.toString();
                    }
                }
            }
            
            if (result == null) {
                result = payload + ";processed";
            }

            // Send response
            Message responseMsg = new Message();
            responseMsg.messageType = "TASK_COMPLETE";
            responseMsg.studentId = studentId;
            responseMsg.payloadStr = result.toString();

            out.println(responseMsg.toJson());
            System.out.println("[WORKER " + workerId + "] Response sent");

        } catch (Exception e) {
            System.err.println("[WORKER " + workerId + "] Error handling RPC: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleHeartbeat() {
        try {
            Message ackMsg = new Message();
            ackMsg.messageType = "HEARTBEAT_ACK";
            ackMsg.studentId = studentId;
            ackMsg.payloadStr = "alive";

            out.println(ackMsg.toJson());
            System.out.println("[WORKER " + workerId + "] Heartbeat ACK sent");
        } catch (Exception e) {
            System.err.println("[WORKER " + workerId + "] Heartbeat error: " + e.getMessage());
        }
    }

    public void disconnect() {
        running = false;
        try {
            if (masterSocket != null) {
                masterSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ReferenceWorker w = new ReferenceWorker();
        w.connect();
        w.execute();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
