package normal;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class Main {

    public static final int NUM_ACCOUNTS = 5000001;

    public static final int NUM_RELATIONS = 3667136;

    public static final int RESULT_SIZE = 96797390;

    public static final int ACCOUNT_SIZE = 128888916;

    public static final int RELATION_SIZE = 185826552;

    public static int NUM_THREADS = 5;

    public static final int ACCOUNT_NAME_SIZE = 16;

    public static final String ACCOUNTS_FILE = "accounts.txt";

    public static final String RELATIONS_FILE = "relations.txt";

    public static final String RESULT_FILE = "result.txt";

    //public static int[][] edges;

    public static int[][][] edges;

    private static int[] edgesNum;

    private static int[] relationArray;

    private static int[] counts;

    private static int[] lineInx;

    private static int[] inxInLine;

    private static byte[] accountBuf;

    private static int[] accountInx;

    private static byte[] resultBuf;


    public static void main(String[] args) throws IOException, InterruptedException {

        Thread thread1 = new Thread(Main::init);
        thread1.start();

        Timer timer = new Timer();
        /*if (args.length == 0 || !args[args.length - 1].equals("debug")) {
            Logger.quiet();
        }*/
        readEnv();
        Logger.info("read env end. elapsed:%.3f ms\n", timer.getTimeAndReset());
        int accountThreadsNum = 5;
        int relationThreadsNum = 5;

        //readRelationFile(relationThreadsNum);
        Thread thread2 = new Thread(() -> {
            try {
                Timer timer2 = new Timer();
                readRelationFileByMmap(relationThreadsNum);
                Logger.info("read relations file. elapsed:%.3f ms\n", timer2.getTime());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        Thread thread3 = new Thread(() -> {
            try {
                Timer timer2 = new Timer();
                //readAccountFile(accountThreadsNum);
                readAccountFileByMmap(accountThreadsNum);
                Logger.info("read accounts file. elapsed:%.3f ms\n", timer2.getTime());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread2.start();
        thread3.start();

        thread2.join();
        initRelation(relationThreadsNum);
        Logger.info("init relation. elapsed:%.3f ms\n", timer.getTimeAndReset());

        calc();
        Logger.info("calc end. elapsed:%.3f ms\n", timer.getTimeAndReset());
        initFileInx();
        Logger.info("init file Index. elapsed:%.3f ms\n", timer.getTimeAndReset());
        thread3.join();

        writeResult(4); // 5
        Logger.info("write result. elapsed:%.3f ms\n", timer.getTimeAndReset());

        Logger.info("total time:%.3f ms\n", timer.totalTime());
    }

    private static void init() {
        Timer timer = new Timer();
        //edges = new int[NUM_RELATIONS][2];
        edgesNum = new int[NUM_THREADS];
        edges = new int[2][NUM_THREADS][NUM_RELATIONS / 3 + 10000];
        accountInx = new int[NUM_ACCOUNTS];
        accountBuf = new byte[ACCOUNT_SIZE];
        relationArray = new int[NUM_ACCOUNTS];
        counts = new int[NUM_ACCOUNTS];
        lineInx = new int[NUM_ACCOUNTS + 1];
        inxInLine = new int[NUM_ACCOUNTS];

        resultBuf = new byte[RESULT_SIZE];
        Logger.info("init time :%.3f ms \n", timer.getTime());
    }

    private static void calc() {
        boolean flag = true;
        while (flag) {
            flag = false;
            for (int i = NUM_ACCOUNTS - 1; i >= 0; i--) {
                if (relationArray[i] != -1) {
                    if (relationArray[relationArray[i]] != relationArray[i]) {
                        relationArray[i] = relationArray[relationArray[i]];
                        flag = true;
                    }
                }
            }
        }
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            if (relationArray[i] != -1) {
                if (relationArray[i] > i && relationArray[relationArray[i]] > i) {
                    relationArray[relationArray[i]] = i;
                    relationArray[i] = i;
                } else if (relationArray[i] > i) {
                    relationArray[i] = relationArray[relationArray[i]];
                } else {
                    relationArray[i] = relationArray[relationArray[i]];
                }
            }
        }
    }

    private static void initRelation(int numThreads) throws InterruptedException {
        while (relationArray == null) {
            Thread.sleep(1);
        }
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            relationArray[i] = -1;
        }

        /*for (int i = 0; i < NUM_RELATIONS; i++) {
            int srcId = edges[i][0];
            int dstId = edges[i][1];
            if (relationArray[srcId] == -1 && relationArray[dstId] == -1) {
                int min = Math.min(srcId, dstId);
                relationArray[srcId] = min;
                relationArray[dstId] = min;
            } else if (relationArray[srcId] == -1 && relationArray[dstId] != -1) {
                relationArray[srcId] = relationArray[dstId];
            } else if (relationArray[srcId] != -1 && relationArray[dstId] == -1) {
                relationArray[dstId] = relationArray[srcId];
            } else {
                if (relationArray[dstId] < relationArray[srcId]) {
                    relationArray[srcId] = relationArray[dstId];
                } else {
                    relationArray[dstId] = relationArray[srcId];
                }
            }
        }*/
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < edgesNum[i]; j++) {
                int srcId = edges[0][i][j];
                int dstId = edges[1][i][j];
                if (relationArray[srcId] == -1 && relationArray[dstId] == -1) {
                    int min = Math.min(srcId, dstId);
                    relationArray[srcId] = min;
                    relationArray[dstId] = min;
                } else if (relationArray[srcId] == -1 && relationArray[dstId] != -1) {
                    relationArray[srcId] = relationArray[dstId];
                } else if (relationArray[srcId] != -1 && relationArray[dstId] == -1) {
                    relationArray[dstId] = relationArray[srcId];
                } else {
                    if (relationArray[dstId] < relationArray[srcId]) {
                        relationArray[srcId] = relationArray[dstId];
                    } else {
                        relationArray[dstId] = relationArray[srcId];
                    }
                }
            }
        }
    }

    private static int getDigits(int x) {
        if (x >= 0 && x <= 9) return 1;
        else if (x >= 10 && x <= 99) return 2;
        else if (x >= 100 && x <= 999) return 3;
        else if (x >= 1000 && x <= 9999) return 4;
        else if (x >= 10000 && x <= 99999) return 5;
        else if (x >= 100000 && x <= 999999) return 6;
        else return 7;
    }

    private static byte[] toBytes(int x) {
        int n = getDigits(x);
        byte[] b = new byte[n];
        for (int i = n - 1; i >= 0; i--) {
            b[i] = (byte)((x % 10) + 48);
            x /= 10;
        }
        return b;
    }

    private static void initFileInx() throws InterruptedException {
        while (counts == null || inxInLine == null) {
            Thread.sleep(1);
        }
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            int id = relationArray[i];
            if (id == -1) {
                counts[i] = 1;
                inxInLine[i] = 1;
            }
            else {
                counts[id]++;
                inxInLine[i] = counts[id];
            }
        }

        int beforeInx = 0;
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            if (inxInLine[i] == 1) {
                lineInx[i] = beforeInx;
                beforeInx = beforeInx + getDigits(i) + counts[i] * 17 + 2;
            }
            else {
                lineInx[i] = lineInx[relationArray[i]];
            }
        }
    }

    private static void writeResult(int numThreads) throws IOException, InterruptedException {
        while (resultBuf == null) {
            Thread.sleep(10);
        }
        int avg = NUM_ACCOUNTS / numThreads;
        int[] threadBase = new int[numThreads + 1];
        for (int i = 0; i < numThreads; i++) {
            threadBase[i + 1] = threadBase[i] + avg;
        }
        threadBase[numThreads] = NUM_ACCOUNTS;

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            int tid = t;
            threads[t] = new Thread(() -> {

                for (int i = threadBase[tid]; i < threadBase[tid + 1]; i++) {
                    if (counts[i] != 0) {
                        int inx = lineInx[i];
                        int digit = getDigits(i);
                        System.arraycopy(toBytes(i), 0, resultBuf, inx, digit);
                        inx += digit;
                        resultBuf[inx] = (byte)32;
                        inx++;
                        int tmp = accountInx[i];
                        System.arraycopy(accountBuf, accountInx[i], resultBuf, inx, ACCOUNT_NAME_SIZE);
                        inx += ACCOUNT_NAME_SIZE;
                        if (counts[i] == 1) {
                            resultBuf[inx++] = (byte)13;
                            resultBuf[inx] = (byte)10;
                        }
                        else {
                            resultBuf[inx] = (byte)44;
                        }
                    }
                    else {
                        int inx = lineInx[i] + getDigits(relationArray[i]) + 1 + (inxInLine[i] - 1) * 17;
                        System.arraycopy(accountBuf, accountInx[i], resultBuf, inx, ACCOUNT_NAME_SIZE);
                        inx += ACCOUNT_NAME_SIZE;
                        if (inxInLine[i] != counts[relationArray[i]]) {
                            resultBuf[inx] = (byte)44;
                        } else {
                            resultBuf[inx++] = (byte)13;
                            resultBuf[inx] = (byte)10;
                        }
                    }
                }
            });

            threads[t].start();
        }

        for (int i = 0; i < numThreads; i++)
            threads[i].join();

        avg = RESULT_SIZE / numThreads;
        for (int i = 0; i < numThreads; i++) {
            threadBase[i + 1] = threadBase[i] + avg;
        }
        threadBase[numThreads] = RESULT_SIZE;

        for (int t = 0; t < numThreads; t++) {
            int tid = t;
            threads[t] = new Thread(() -> {
                FileChannel fileChannel;
                MappedByteBuffer mmap;
                try {
                    fileChannel = new RandomAccessFile(RESULT_FILE, "rw").getChannel();
                    mmap = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, RESULT_SIZE);
                    mmap.position(threadBase[tid]);
                    mmap.put(resultBuf, threadBase[tid], threadBase[tid + 1] - threadBase[tid]);
                    //mmap.force();
                }catch (IOException e) {
                    e.printStackTrace();
                }
            });
            threads[t].start();
        }
        for (int i = 0; i < numThreads; i++)
            threads[i].join();

    }

    public static void readAccountFile(int numThreads) throws IOException, InterruptedException {
        File file = new File(ACCOUNTS_FILE);
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));

        bis.read(accountBuf);

        int[] threadBase = new int[numThreads + 1];
        int avg = (int) ACCOUNT_SIZE / numThreads;
        threadBase[0] = 0;
        for (int i = 0; i < numThreads; i++) {
            int endInx = (i + 1) * avg - 1;
            while (endInx < ACCOUNT_SIZE && accountBuf[endInx] != '\n') {
                endInx++;
            }
            threadBase[i + 1] = endInx;
        }

        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            int tid = i;
            threads[i] = new Thread(() -> {
                for (int inx = threadBase[tid]; inx < threadBase[tid + 1]; inx++) {

                    while (inx < threadBase[tid + 1] && (accountBuf[inx] < '0' || accountBuf[inx] > '9')) {
                        inx++;
                    }
                    if (inx >= threadBase[tid + 1]) {
                        break;
                    }

                    int id = 0;
                    while (accountBuf[inx] >= '0' && accountBuf[inx] <= '9') {
                        id = id * 10 + accountBuf[inx] - '0';
                        inx++;
                    }

                    inx++;
                    accountInx[id] = inx;
                    inx += 17;
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        bis.close();
    }

    private static void readAccountFileByMmap(int numThreads) throws IOException, InterruptedException {
        FileChannel channel = new FileInputStream(ACCOUNTS_FILE).getChannel();

        MappedByteBuffer accountFileMap = channel.map(FileChannel.MapMode.READ_ONLY, 0, ACCOUNT_SIZE);

        while (accountBuf == null) {
            Thread.sleep(3);
        }
        accountFileMap.get(accountBuf);

        int[] threadBase = new int[numThreads + 1];

        int avg = ACCOUNT_SIZE / numThreads;
        threadBase[0] = 0;
        for (int i = 0; i < numThreads; i++) {
            int endInx = (i + 1) * avg - 1;
            while (endInx < ACCOUNT_SIZE && accountBuf[endInx] != '\n') {
                endInx++;
            }
            threadBase[i + 1] = endInx;
        }

        Thread[] threads = new Thread[numThreads];

        while (accountInx == null) {
            Thread.sleep(3);
        }
        for (int i = 0; i < numThreads; i++) {
            int tid = i;
            threads[tid] = new Thread(() -> {
                for (int inx = threadBase[tid]; inx < threadBase[tid + 1]; inx++) {

                    while (inx < threadBase[tid + 1] && (accountBuf[inx] < '0' || accountBuf[inx] > '9')) {
                        inx++;
                    }
                    if (inx >= threadBase[tid + 1]) {
                        break;
                    }
                    int id = 0;
                    while (accountBuf[inx] >= '0' && accountBuf[inx] <= '9') {
                        id = id * 10 + accountBuf[inx] - '0';
                        inx++;
                    }
                    inx++;
                    accountInx[id] = inx;
                    inx+= 16;
                }
            });
            threads[tid].start();

        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public static void readRelationFileByMmap(int numThreads) throws IOException, InterruptedException {
        FileChannel fileChannel = new FileInputStream(RELATIONS_FILE).getChannel();
        long fileSize = fileChannel.size();
        MappedByteBuffer buff = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

        int[] threadBase = new int[numThreads + 1];

        int avg = RELATION_SIZE / numThreads;
        threadBase[0] = 0;
        for (int i = 0; i < numThreads; i++) {
            int endInx = (i + 1) * avg - 1;
            while (endInx < RELATION_SIZE && buff.get(endInx) != '\n') {
                endInx++;
            }
            threadBase[i + 1] = endInx;
        }

        Thread[] threads = new Thread[numThreads];
        while (edges == null) {
            Thread.sleep(3);
        }
        for (int i = 0; i < numThreads; i++) {
            int tid = i;
            threads[tid] = new Thread(() -> {

                int n = 0;
                for (int inx = threadBase[tid]; inx < threadBase[tid + 1]; inx++) {
                    while (inx < fileSize && (buff.get(inx) < '0' || buff.get(inx) > '9')) {
                        inx++;
                    }
                    if (inx >= fileSize) {
                        break;
                    }
                    int srcId = 0;
                    while (buff.get(inx) >= '0' && buff.get(inx) <= '9') {
                        srcId = srcId * 10 + buff.get(inx) - '0';
                        inx++;
                    }
                    inx += 18;

                    int dstId = 0;
                    while (buff.get(inx) >= '0' && buff.get(inx) <= '9') {
                        dstId = dstId * 10 + buff.get(inx++) - '0';
                    }
                    inx += 18;

                    edges[0][tid][n] = srcId;
                    edges[1][tid][n++] = dstId;
                }
                edgesNum[tid] = n;
            });
            threads[tid].start();
        }

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }


    public static void readRelationFile(int numThreads) throws IOException, InterruptedException {

        FileChannel fileChannel = new FileInputStream(RELATIONS_FILE).getChannel();
        long fileSize = fileChannel.size();
        MappedByteBuffer buff = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        int[] lines = new int[numThreads];

        Timer timer = new Timer();
        ReadLineThread[] threads = new ReadLineThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            int start = (int) (fileSize * i / numThreads);
            int end = (int) (fileSize * (i + 1) / numThreads);
            if (i != 0) {
                while (buff.get(start) != '\n') {
                    ++start;
                }
                ++start;
            }
            if (i + 1 != numThreads) {
                while (buff.get(end) != '\n') {
                    ++end;
                }
                ++end;
            }

            threads[i] = new ReadLineThread(start, end, buff);
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        int line_sum = 0;
        for (int i = 1; i < numThreads; i++) {
            lines[i] = threads[i - 1].line + lines[i - 1];
            line_sum += threads[i - 1].line;
        }
        line_sum += threads[numThreads - 1].line;
        Logger.info("read line. time:%.3f ms \n", timer.getTime());
        ReadRelationThread[] relationThreads = new ReadRelationThread[numThreads];
        while (edges == null) {
            Thread.sleep(3);
        }
        for (int i = 0; i < numThreads; i++) {
            int start = (int) (fileSize * i / numThreads);
            int end = (int) (fileSize * (i + 1) / numThreads);
            if (i != 0) {
                while (buff.get(start) != '\n') {
                    ++start;
                }
                ++start;
            }
            if (i + 1 != numThreads) {
                while (buff.get(end) != '\n') {
                    ++end;
                }
                ++end;
            }
            relationThreads[i] = new ReadRelationThread(lines[i], start, end, buff);
            relationThreads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            relationThreads[i].join();
        }
    }

    private static void readEnv() {

        NUM_THREADS = Runtime.getRuntime().availableProcessors();
        Logger.info("numCpu:%d\n", NUM_THREADS);
    }
}
