package pdc;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    private int port;
    private ServerSocket serverSocket;
    private ConcurrentMap<Integer, WorkerConnection> workers = new ConcurrentHashMap<>();
    private AtomicInteger workerIdCounter = new AtomicInteger(0);
    private ExecutorService threadPool;
    private String studentId;
    private volatile boolean running = false;
    
    // Task tracking
    private BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private ConcurrentMap<Integer, Task> activeTasks = new ConcurrentHashMap<>();
    private AtomicInteger taskIdCounter = new AtomicInteger(0);
    
    // Matrix computation
    private int[][] resultMatrix;
    private ConcurrentMap<Integer, int[]> rowResults = new ConcurrentHashMap<>();

    public Master() throws IOException {
        this(5000);
    }

    public Master(int port) throws IOException {
        this.port = port;
        this.studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "DEFAULT_STUDENT";
        }
        this.threadPool = Executors.newFixedThreadPool(10);
        this.serverSocket = new ServerSocket(port);
        this.running = true;
        System.out.println("Master initialized on port " + port);
    }

    public void start() {
        System.out.println("Master listening on port " + port);
        threadPool.execute(() -> acceptWorkers());
    }

    private void acceptWorkers() {
        try {
            while (running) {
                Socket workerSocket = serverSocket.accept();
                System.out.println("Worker connected from " + workerSocket.getInetAddress());
                int workerId = workerIdCounter.getAndIncrement();
                
                WorkerConnection conn = new WorkerConnection(workerId, workerSocket);
                workers.put(workerId, conn);
                
                threadPool.execute(() -> handleWorkerConnection(conn));
            }
        } catch (IOException e) {
            if (running) {
                e.printStackTrace();
            }
        }
    }

    private void handleWorkerConnection(WorkerConnection conn) {
        try {
            DataInputStream in = new DataInputStream(conn.socket.getInputStream());
            DataOutputStream out = new DataOutputStream(conn.socket.getOutputStream());
            
            conn.in = in;
            conn.out = out;
            
            // Wait for REGISTER_WORKER message
            byte[] registerData = readMessage(in);
            if (registerData != null) {
                Message registerMsg = Message.unpack(registerData);
                if ("REGISTER_WORKER".equals(registerMsg.type)) {
                    System.out.println("Worker " + conn.workerId + " registered");
                    conn.registered = true;
                    
                    // Send WORKER_ACK
                    Message ackMsg = new Message();
                    ackMsg.magic = "CSM218";
                    ackMsg.version = 1;
                    ackMsg.type = "WORKER_ACK";
                    ackMsg.sender = "MASTER";
                    ackMsg.timestamp = System.currentTimeMillis();
                    ackMsg.payload = ("" + conn.workerId).getBytes();
                    
                    out.write(ackMsg.pack());
                    out.flush();
                    
                    // Listen for messages from worker
                    listenToWorker(conn, in, out);
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling worker " + conn.workerId + ": " + e.getMessage());
        } finally {
            workers.remove(conn.workerId);
            conn.alive = false;
            try {
                conn.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void listenToWorker(WorkerConnection conn, DataInputStream in, DataOutputStream out) {
        try {
            while (running && conn.alive) {
                byte[] data = readMessage(in);
                if (data == null) break;
                
                Message msg = Message.unpack(data);
                System.out.println("Master received " + msg.type + " from worker " + conn.workerId);
                
                if ("TASK_COMPLETE".equals(msg.type)) {
                    handleTaskComplete(conn, msg);
                } else if ("HEARTBEAT_ACK".equals(msg.type)) {
                    conn.lastHeartbeatAck = System.currentTimeMillis();
                }
            }
        } catch (IOException e) {
            System.err.println("Worker " + conn.workerId + " disconnected");
        }
    }

    private void handleTaskComplete(WorkerConnection conn, Message msg) {
        try {
            int taskId = Integer.parseInt(new String(msg.payload).split(":")[0]);
            Task task = activeTasks.get(taskId);
            if (task != null) {
                // Extract row result from payload
                String[] parts = new String(msg.payload).split(":");
                if (parts.length >= 2) {
                    int rowIndex = task.rowIndex;
                    String[] rowData = parts[1].split(",");
                    int[] row = new int[rowData.length];
                    for (int i = 0; i < rowData.length; i++) {
                        row[i] = Integer.parseInt(rowData[i]);
                    }
                    rowResults.put(rowIndex, row);
                    activeTasks.remove(taskId);
                    System.out.println("Task " + taskId + " completed for row " + rowIndex);
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing task complete: " + e.getMessage());
        }
    }

    public void sendTask(WorkerConnection conn, Task task) {
        try {
            Message msg = new Message();
            msg.magic = "CSM218";
            msg.version = 1;
            msg.type = "RPC_REQUEST";
            msg.sender = "MASTER";
            msg.timestamp = System.currentTimeMillis();
            
            // Payload: taskId|rowIndex|left_matrix|right_matrix
            String payload = task.taskId + "|" + task.rowIndex + "|" + 
                           matrixToString(task.leftMatrix) + "|" + 
                           matrixToString(task.rightMatrix);
            msg.payload = payload.getBytes();
            
            conn.out.write(msg.pack());
            conn.out.flush();
            
            activeTasks.put(task.taskId, task);
            System.out.println("Sent task " + task.taskId + " to worker " + conn.workerId);
        } catch (IOException e) {
            System.err.println("Error sending task to worker " + conn.workerId + ": " + e.getMessage());
        }
    }

    public void broadcastHeartbeat() {
        for (WorkerConnection conn : workers.values()) {
            if (conn.alive && conn.registered) {
                try {
                    Message hb = new Message();
                    hb.magic = "CSM218";
                    hb.version = 1;
                    hb.type = "HEARTBEAT";
                    hb.sender = "MASTER";
                    hb.timestamp = System.currentTimeMillis();
                    hb.payload = new byte[0];
                    
                    conn.out.write(hb.pack());
                    conn.out.flush();
                } catch (IOException e) {
                    conn.alive = false;
                }
            }
        }
    }

    private byte[] readMessage(DataInputStream in) throws IOException {
        try {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            return data;
        } catch (EOFException e) {
            return null;
        }
    }

    // ===== AUTOGRADER METHODS =====
    public Object coordinate(String op, int[][] matrix, int numWorkers) {
        System.out.println("coordinate() called with op=" + op + ", matrix=" + 
                         (matrix != null ? matrix.length + "x" + matrix[0].length : "null") + 
                         ", numWorkers=" + numWorkers);
        
        if (matrix == null || matrix.length == 0) {
            return null;
        }
        
        try {
            // Wait for workers to connect
            long startWait = System.currentTimeMillis();
            while (workers.size() < numWorkers && System.currentTimeMillis() - startWait < 5000) {
                Thread.sleep(100);
            }
            
            System.out.println("Available workers: " + workers.size());
            
            // For matrix multiplication, distribute rows in parallel
            if ("MATMUL".equals(op)) {
                int rows = matrix.length;
                int cols = matrix[0].length;
                
                resultMatrix = new int[rows][cols];
                rowResults.clear();
                activeTasks.clear();
                
                // Create callable tasks for each row
                List<java.util.concurrent.Callable<Void>> tasks = new ArrayList<>();
                for (int row = 0; row < rows; row++) {
                    final int rowIndex = row;
                    tasks.add(() -> {
                        Task task = new Task();
                        task.taskId = taskIdCounter.getAndIncrement();
                        task.rowIndex = rowIndex;
                        task.leftMatrix = new int[][] { matrix[rowIndex] };
                        task.rightMatrix = matrix;
                        
                        // Find available worker and send task
                        boolean sent = false;
                        for (WorkerConnection conn : workers.values()) {
                            if (conn.alive && conn.registered) {
                                sendTask(conn, task);
                                sent = true;
                                break;
                            }
                        }
                        
                        if (!sent) {
                            System.err.println("No available workers for task " + task.taskId);
                            taskQueue.offer(task);
                        }
                        return null;
                    });
                }
                
                // Execute all tasks in parallel
                try {
                    List<java.util.concurrent.Future<Void>> futures = threadPool.invokeAll(tasks, 30, java.util.concurrent.TimeUnit.SECONDS);
                    System.out.println("All task submissions completed");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Task submission interrupted");
                }
                
                // Wait for all results to come back
                long timeout = System.currentTimeMillis() + 30000;
                while (rowResults.size() < rows && System.currentTimeMillis() < timeout) {
                    Thread.sleep(100);
                    
                    // Reassign any incomplete tasks
                    for (Task task : activeTasks.values()) {
                        if (System.currentTimeMillis() - task.submissionTime > 5000) {
                            System.out.println("Task " + task.taskId + " timeout, reassigning");
                            taskQueue.offer(task);
                            activeTasks.remove(task.taskId);
                        }
                    }
                }
                
                // Assemble results
                for (int i = 0; i < rows; i++) {
                    if (rowResults.containsKey(i)) {
                        resultMatrix[i] = rowResults.get(i);
                    }
                }
                
                System.out.println("Computation complete. Result rows: " + rowResults.size());
                return resultMatrix;
            }
            
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private void distributeTasks() {
        try {
            while (!taskQueue.isEmpty()) {
                Task task = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task != null) {
                    // Find available worker
                    for (WorkerConnection conn : workers.values()) {
                        if (conn.alive && conn.registered) {
                            sendTask(conn, task);
                            break;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void listen(int listenPort) {
        System.out.println("listen() called on port " + listenPort);
        try {
            this.port = listenPort;
            this.serverSocket = new ServerSocket(listenPort);
            start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reconcileState() {
        System.out.println("reconcileState() called");
        // Reassign any incomplete tasks
        for (Task task : activeTasks.values()) {
            taskQueue.offer(task);
        }
        activeTasks.clear();
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

    private String matrixToString(int[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                sb.append(matrix[i][j]);
                if (j < matrix[i].length - 1) sb.append(",");
            }
            if (i < matrix.length - 1) sb.append(";");
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        int port = 5000;
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null) {
            port = Integer.parseInt(portEnv);
        }
        
        Master master = new Master(port);
        master.start();
        
        // Keep master running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Inner class for worker connection state
    static class WorkerConnection {
        int workerId;
        Socket socket;
        DataInputStream in;
        DataOutputStream out;
        boolean registered = false;
        boolean alive = true;
        long lastHeartbeatAck = System.currentTimeMillis();

        WorkerConnection(int workerId, Socket socket) {
            this.workerId = workerId;
            this.socket = socket;
        }
    }

    // Inner class for task representation
    static class Task {
        int taskId;
        int rowIndex;
        int[][] leftMatrix;
        int[][] rightMatrix;
        long submissionTime;
        
        Task() {
            this.submissionTime = System.currentTimeMillis();
        }
    }
}
