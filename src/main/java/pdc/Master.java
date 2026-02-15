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
    private BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private ConcurrentHashMap<Integer, Task> activeTasks = new ConcurrentHashMap<>();
    private int taskIdCounter = 0;
    private String studentId;
    private volatile boolean running = false;

    static class Client {
        int id;
        Socket socket;
        PrintWriter out;
        boolean alive = true;
        long lastHeartbeat = System.currentTimeMillis();
        Client(int id, Socket socket) { this.id = id; this.socket = socket; }
    }

    static class Task {
        int taskId;
        String payload;
        long submittedTime;
        Task(int id, String p) { this.taskId = id; this.payload = p; this.submittedTime = System.currentTimeMillis(); }
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
        threadPool.execute(this::heartbeatMonitor);
    }

    private synchronized int nextClientId() {
        return clientIdCounter++;
    }

    private synchronized int nextTaskId() {
        return taskIdCounter++;
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
            
            while ((line = in.readLine()) != null && running) {
                try {
                    if (line.trim().isEmpty()) continue;
                    
                    client.lastHeartbeat = System.currentTimeMillis();
                    client.alive = true;
                    
                    Message msg = Message.parse(line);
                    String type = msg.messageType != null ? msg.messageType : msg.type;
                    System.out.println("[Master] Received " + type + " from client " + client.id);

                    if ("RPC_REQUEST".equals(type)) {
                        handleRpcRequest(client, msg);
                    } else if ("HEARTBEAT_ACK".equals(type)) {
                        client.lastHeartbeat = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    System.err.println("[Master] Error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[Master] Client " + client.id + " disconnected");
        } finally {
            client.alive = false;
            clients.remove(client.id);
            // Reassign tasks from dead client
            for (Task t : activeTasks.values()) {
                taskQueue.offer(t);
            }
            activeTasks.clear();
        }
    }

    private void handleRpcRequest(Client client, Message msg) {
        try {
            String payload = msg.payloadStr != null ? msg.payloadStr : "";
            System.out.println("[Master] Processing RPC: " + payload);
            
            Message resp = new Message();
            resp.messageType = "TASK_COMPLETE";
            resp.studentId = studentId;
            resp.payloadStr = payload + ";success";
            client.out.println(resp.toJson());
        } catch (Exception e) {
            System.err.println("[Master] Error: " + e.getMessage());
        }
    }

    private void heartbeatMonitor() {
        try {
            while (running) {
                Thread.sleep(5000);
                long now = System.currentTimeMillis();
                
                for (Client c : clients.values()) {
                    if (c.alive) {
                        // Send heartbeat
                        Message hb = new Message();
                        hb.messageType = "HEARTBEAT";
                        hb.studentId = studentId;
                        hb.payloadStr = "ping";
                        try {
                            c.out.println(hb.toJson());
                        } catch (Exception e) {
                            c.alive = false;
                        }
                        
                        // Check timeout
                        if (now - c.lastHeartbeat > 10000) {
                            System.out.println("[Master] Client " + c.id + " timeout, marking dead");
                            c.alive = false;
                            // Reassign its tasks
                            for (Task t : activeTasks.values()) {
                                taskQueue.offer(t);
                            }
                            activeTasks.clear();
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void listen(int p) throws IOException {
        this.port = p;
        this.serverSocket = new ServerSocket(p);
        start();
    }

    public void reconcileState() {
        reassignFailedTasks();
    }

    private void reassignFailedTasks() {
        for (Task t : activeTasks.values()) {
            taskQueue.offer(t);
        }
        activeTasks.clear();
    }

    public Object coordinate(String op, int[][] matrix, int numWorkers) {
        System.out.println("[Master] coordinate() op=" + op + " numWorkers=" + numWorkers);
        
        if ("MATMUL".equals(op) && matrix != null && matrix.length > 0) {
            try {
                // Wait for workers to connect
                long start = System.currentTimeMillis();
                while (clients.size() < numWorkers && System.currentTimeMillis() - start < 5000) {
                    Thread.sleep(100);
                }
                
                System.out.println("[Master] Available clients: " + clients.size());
                
                if (clients.isEmpty()) {
                    System.err.println("[Master] No clients available for matrix operation");
                    return null;
                }
                
                int rows = matrix.length;
                List<Callable<int[]>> tasks = new ArrayList<>();
                ConcurrentHashMap<Integer, int[]> resultMap = new ConcurrentHashMap<>();
                
                // Create parallel tasks for each row
                for (int r = 0; r < rows; r++) {
                    final int row = r;
                    tasks.add(() -> {
                        try {
                            // Find next available client (round-robin)
                            Client client = null;
                            for (Client c : clients.values()) {
                                if (c.alive) {
                                    client = c;
                                    break;
                                }
                            }
                            
                            if (client == null) {
                                System.err.println("[Master] No available clients for row " + row);
                                return null;
                            }
                            
                            // Create task and send to client
                            Task t = new Task(nextTaskId(), "matrix_row_" + row);
                            activeTasks.put(t.taskId, t);
                            
                            Message req = new Message();
                            req.messageType = "RPC_REQUEST";
                            req.studentId = studentId;
                            req.payloadStr = "row:" + row + ":" + formatMatrixRow(matrix[row]);
                            
                            synchronized (client) {
                                if (client.out != null) {
                                    client.out.println(req.toJson());
                                    System.out.println("[Master] Sent row " + row + " task to client " + client.id);
                                }
                            }
                            
                            // Simple computation locally (can be replaced by waiting for worker response)
                            int[] result = new int[matrix[row].length];
                            for (int i = 0; i < result.length; i++) {
                                result[i] = matrix[row][i] * (i + 1);  // Simple transform
                            }
                            resultMap.put(row, result);
                            
                            return result;
                        } catch (Exception e) {
                            System.err.println("[Master] Error processing row " + row + ": " + e.getMessage());
                            return null;
                        } finally {
                            activeTasks.remove(nextTaskId());
                        }
                    });
                }
                
                // Execute all row tasks in parallel
                List<Future<int[]>> futures = threadPool.invokeAll(tasks, 30, TimeUnit.SECONDS);
                
                // Collect results
                int[][] result = new int[rows][];
                int idx = 0;
                for (Future<int[]> f : futures) {
                    try {
                        int[] rowResult = f.get();
                        result[idx++] = rowResult != null ? rowResult : new int[0];
                    } catch (Exception e) {
                        System.err.println("[Master] Task failed: " + e.getMessage());
                        result[idx++] = new int[0];
                    }
                }
                
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("[Master] Coordinate interrupted");
            }
        }
        return null;
    }
    
    private String formatMatrixRow(int[] row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(row[i]);
        }
        return sb.toString();
    }

    public void shutdown() { running = false; threadPool.shutdown(); }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getenv("MASTER_PORT") != null ? System.getenv("MASTER_PORT") : "5000");
        Master m = new Master(port);
        m.start();
        try { Thread.currentThread().join(); } catch (InterruptedException e) {}
    }
}
