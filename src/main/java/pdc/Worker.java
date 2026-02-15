package pdc;

import java.io.*;
import java.net.*;

public class Worker {

    private Socket masterSocket;
    private DataInputStream in;
    private DataOutputStream out;
    private String workerId;
    private String masterHost;
    private int masterPort;
    private String studentId;
    private volatile boolean running = false;

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "DEFAULT_STUDENT";
        }
    }

    public Worker() {
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

    public void joinCluster(String host, int port) {
        this.masterHost = host;
        this.masterPort = port;
        connect();
    }

    public void connect() {
        try {
            masterSocket = new Socket(masterHost, masterPort);
            in = new DataInputStream(masterSocket.getInputStream());
            out = new DataOutputStream(masterSocket.getOutputStream());
            
            System.out.println("Worker " + workerId + " connected to master at " + 
                             masterHost + ":" + masterPort);
            
            running = true;
            registerWithMaster();
            
        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void registerWithMaster() {
        try {
            // Create REGISTER_WORKER message
            Message regMsg = new Message();
            regMsg.magic = "CSM218";
            regMsg.version = 1;
            regMsg.type = "REGISTER_WORKER";
            regMsg.sender = workerId;
            regMsg.timestamp = System.currentTimeMillis();
            regMsg.payload = workerId.getBytes();
            
            byte[] data = regMsg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            
            System.out.println("Worker " + workerId + " sent REGISTER_WORKER");
            
            // Wait for WORKER_ACK
            byte[] ackData = readMessage();
            if (ackData != null) {
                Message ackMsg = Message.unpack(ackData);
                if ("WORKER_ACK".equals(ackMsg.type)) {
                    System.out.println("Worker " + workerId + " received WORKER_ACK");
                } else {
                    System.err.println("Unexpected message type: " + ackMsg.type);
                }
            }
        } catch (IOException e) {
            System.err.println("Error registering worker: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void execute() {
        new Thread(() -> {
            try {
                while (running) {
                    byte[] msgData = readMessage();
                    if (msgData == null) {
                        System.out.println("Worker " + workerId + " connection closed");
                        running = false;
                        break;
                    }
                    
                    Message msg = Message.unpack(msgData);
                    System.out.println("Worker " + workerId + " received " + msg.type);
                    
                    if ("RPC_REQUEST".equals(msg.type)) {
                        handleRpcRequest(msg);
                    } else if ("HEARTBEAT".equals(msg.type)) {
                        handleHeartbeat(msg);
                    }
                }
            } catch (IOException e) {
                System.err.println("Worker " + workerId + " error: " + e.getMessage());
            }
        }).start();
    }

    private void handleRpcRequest(Message msg) {
        try {
            String payload = new String(msg.payload);
            String[] parts = payload.split("\\|");
            
            if (parts.length < 4) {
                System.err.println("Invalid task payload");
                return;
            }
            
            int taskId = Integer.parseInt(parts[0]);
            int rowIndex = Integer.parseInt(parts[1]);
            int[][] leftMatrix = parseMatrix(parts[2]);
            int[][] rightMatrix = parseMatrix(parts[3]);
            
            System.out.println("Worker " + workerId + " executing task " + taskId + 
                             " for row " + rowIndex);
            
            // Compute row result
            int[] result = multiplyMatrixRow(leftMatrix[0], rightMatrix);
            
            // Send TASK_COMPLETE
            Message completeMsg = new Message();
            completeMsg.magic = "CSM218";
            completeMsg.version = 1;
            completeMsg.type = "TASK_COMPLETE";
            completeMsg.sender = workerId;
            completeMsg.timestamp = System.currentTimeMillis();
            
            // Payload: taskId:resultRow
            StringBuilder resultStr = new StringBuilder();
            resultStr.append(taskId).append(":");
            for (int i = 0; i < result.length; i++) {
                resultStr.append(result[i]);
                if (i < result.length - 1) resultStr.append(",");
            }
            
            completeMsg.payload = resultStr.toString().getBytes();
            
            byte[] data = completeMsg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            
            System.out.println("Worker " + workerId + " sent TASK_COMPLETE for task " + taskId);
            
        } catch (Exception e) {
            System.err.println("Error handling RPC request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleHeartbeat(Message msg) {
        try {
            Message ackMsg = new Message();
            ackMsg.magic = "CSM218";
            ackMsg.version = 1;
            ackMsg.type = "HEARTBEAT_ACK";
            ackMsg.sender = workerId;
            ackMsg.timestamp = System.currentTimeMillis();
            ackMsg.payload = new byte[0];
            
            byte[] data = ackMsg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            
            System.out.println("Worker " + workerId + " sent HEARTBEAT_ACK");
        } catch (IOException e) {
            System.err.println("Error sending heartbeat ack: " + e.getMessage());
        }
    }

    private int[] multiplyMatrixRow(int[] row, int[][] rightMatrix) {
        int cols = rightMatrix[0].length;
        int[] result = new int[cols];
        
        for (int col = 0; col < cols; col++) {
            int sum = 0;
            for (int k = 0; k < row.length; k++) {
                sum += row[k] * rightMatrix[k][col];
            }
            result[col] = sum;
        }
        
        return result;
    }

    private int[][] parseMatrix(String matrixStr) {
        String[] rows = matrixStr.split(";");
        int[][] matrix = new int[rows.length][];
        
        for (int i = 0; i < rows.length; i++) {
            String[] cols = rows[i].split(",");
            matrix[i] = new int[cols.length];
            for (int j = 0; j < cols.length; j++) {
                matrix[i][j] = Integer.parseInt(cols[j]);
            }
        }
        
        return matrix;
    }

    private byte[] readMessage() throws IOException {
        try {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            return data;
        } catch (EOFException e) {
            return null;
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
        Worker w = new Worker();
        w.connect();
        w.execute();
        
        // Keep worker running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
