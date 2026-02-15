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
    private BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private ConcurrentMap<Integer, Task> activeTasks = new ConcurrentHashMap<>();
    private AtomicInteger taskIdCounter = new AtomicInteger(0);

    static class Task {
        int taskId;
        String type;
        String payload;
        long submittedAt;
        Task(int id, String type, String payload) {
            this.taskId = id;
            this.type = type;
            this.payload = payload;
            this.submittedAt = System.currentTimeMillis();
        }
    }

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
        threadPool.execute(() -> taskProcessor());
    }

    private void taskProcessor() {
        try {
            while (running) {
                Task task = taskQueue.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    processTask(task);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processTask(Task task) {
        try {
            Object result = null;
            
            if ("MATRIX_MULTIPLY".equals(task.type) || "MATMUL".equals(task.type)) {
                result = computeMatrixOp(task.payload);
            } else {
                result = task.payload + ";processed";
            }
            
            // Send result to first available client (simple approach)
            for (ClientConnection c : clients.values()) {
                if (c.out != null) {
                    Message response = new Message();
                    response.messageType = "TASK_COMPLETE";
                    response.studentId = studentId;
                    response.payloadStr = result != null ? result.toString() : "null";
                    try {
                        c.out.println(response.toJson());
                    } catch (Exception e) {
                        // Client connection failed
                    }
                    break;
                }
            }
            
            activeTasks.remove(task.taskId);
        } catch (Exception e) {
            System.err.println("[MASTER] Task processing error: " + e.getMessage());
        }
    }

    private Object computeMatrixOp(String payload) {
        try {
            // Parse matrix payload format: "1,2\\3,4|5,6\\7,8"
            // Represents: [[1,2],[3,4]] and [[5,6],[7,8]]
            String[] matrices = payload.split("\\|");
            if (matrices.length != 2) {
                return "error: invalid matrix format";
            }
            
            int[][] matrix1 = parseMatrix(matrices[0]);
            int[][] matrix2 = parseMatrix(matrices[1]);
            
            if (matrix1 == null || matrix2 == null) {
                return "error: failed to parse matrices";
            }
            
            int[][] result = multiplyMatrices(matrix1, matrix2);
            return formatMatrix(result);
        } catch (Exception e) {
            System.err.println("[MASTER] Matrix computation error: " + e.getMessage());
            return "error: " + e.getMessage();
        }
    }

    private int[][] parseMatrix(String matrixStr) {
        try {
            String[] rows = matrixStr.split("\\\\");
            int[][] matrix = new int[rows.length][];
            
            for (int i = 0; i < rows.length; i++) {
                String[] values = rows[i].split(",");
                matrix[i] = new int[values.length];
                for (int j = 0; j < values.length; j++) {
                    matrix[i][j] = Integer.parseInt(values[j].trim());
                }
            }
            
            return matrix;
        } catch (Exception e) {
            System.err.println("[MASTER] Matrix parsing error: " + e.getMessage());
            return null;
        }
    }

    private int[][] multiplyMatrices(int[][] a, int[][] b) {
        int[][] result = new int[a.length][b[0].length];
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < b[0].length; j++) {
                result[i][j] = 0;
                for (int k = 0; k < a[0].length; k++) {
                    result[i][j] += a[i][k] * b[k][j];
                }
            }
        }
        return result;
    }

    private String formatMatrix(int[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) sb.append("\\");
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) sb.append(",");
                sb.append(matrix[i][j]);
            }
        }
        return sb.toString();
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
                    if (line.trim().isEmpty()) continue;
                    
                    Message msg = Message.parse(line);
                    System.out.println("[MASTER] Received " + msg.messageType + " from client " + conn.clientId);

                    if ("RPC_REQUEST".equals(msg.messageType)) {
                        handleRpcRequest(conn, msg);
                    } else if ("TASK_COMPLETE".equals(msg.messageType)) {
                        System.out.println("[MASTER] Task result received: " + msg.payloadStr);
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
            String payload = msg.payloadStr;
            System.out.println("[MASTER] Processing RPC: " + payload);

            // Queue task for processing
            Task task = new Task(taskIdCounter.getAndIncrement(), "RPC_REQUEST", payload);
            activeTasks.put(task.taskId, task);
            taskQueue.offer(task);

            // Send acknowledgment immediately
            Message ack = new Message();
            ack.messageType = "ACK";
            ack.studentId = studentId;
            ack.payloadStr = "task_queued";
            conn.out.println(ack.toJson());
            System.out.println("[MASTER] ACK sent for task " + task.taskId);
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
