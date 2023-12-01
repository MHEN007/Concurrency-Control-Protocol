import java.util.*;

class MVCC {
    private Map<String, List<Map<String, Object>>> versionTable;
    private Deque<Map<String, Object>> sequence;
    private Deque<Map<String, Object>> inputSequence;
    private int counter;
    private int[] transactionCounter;

    public MVCC(List<Map<String, Object>> inputSequence) {
        this.versionTable = new HashMap<>();
        this.sequence = new LinkedList<>();
        this.inputSequence = new LinkedList<>(inputSequence);
        this.counter = 0;
        this.transactionCounter = new int[10]; 

        for (int i = 0; i < 10; i++) {
            transactionCounter[i] = i;
        }
    }

    private int getMaxVersionIndexByWrite(String item) {
        int maxWTimestamp = (int) versionTable.get(item).get(0).get("timestampW");
        int maxIndex = 0;

        for (int i = 0; i < versionTable.get(item).size(); i++) {
            int currentWTimestamp = (int) versionTable.get(item).get(i).get("timestampW");
            if (currentWTimestamp > maxWTimestamp) {
                maxWTimestamp = currentWTimestamp;
                maxIndex = i;
            }
        }

        return maxIndex;
    }

    public void read(int tx, String item) {
        if (!versionTable.containsKey(item)) {
            // Check if tx is in version table
            boolean txInVersionTable = false;

            
            for (Map<String, Object> op : sequence) {
                if (((int) op.get("tx")) == tx && !op.get("action").equals("aborted")) {
                    txInVersionTable = true;
                    break;
                }
            }

            if (!txInVersionTable) {
                System.out.println("kenak 1");
                System.out.println("transaction counter: ");
                for (int i = 0; i < 10; i++) {
                    System.out.print(transactionCounter[i] + ",");
                }
                System.out.println("tx = " + tx);
                System.out.println();
                versionTable.put(item, new ArrayList<>());
                versionTable.get(item).add(Map.of("tx", tx, "timestampR", transactionCounter[tx], "timestampW", 0, "version", 0));
                              sequence.add(Map.of("tx", tx, "item", item, "action", "read", "timestampR", transactionCounter[tx], "timestampW", 0, "version", 0));
                System.out.println("Transaction " + tx + " read " + item + " at version " + 0 + ". Timestamp " + item
                        + " now: (" + transactionCounter[tx] + ", " + 0 + ").");
                this.counter++;
            } else {
                System.out.println("kenak 2");
                System.out.println(item);
                int maxIndex = getMaxVersionIndexByWrite(item);
                int maxWTimestamp = (int) versionTable.get(item).get(maxIndex).get("timestampW");
                int maxRTimestamp = (int) versionTable.get(item).get(maxIndex).get("timestampR");
                int maxVersion = (int) versionTable.get(item).get(maxIndex).get("version");

                if (this.transactionCounter[tx] > maxRTimestamp) {
                    this.versionTable.get(item).get(maxIndex).put("timestampR", this.transactionCounter[tx]);
                    this.versionTable.get(item).get(maxIndex).put("timestampW", maxWTimestamp);
                    
                }

                System.out.println("Transaction " + tx + " read " + item + " at version " + maxVersion + ". Timestamp "
                        + item + " now: (" + maxRTimestamp + ", " + maxWTimestamp + ").");
                
                this.counter++;
            }
        } else {
            System.out.println("kenak 3");
            int maxIndex = getMaxVersionIndexByWrite(item);
            int maxWTimestamp = (int) versionTable.get(item).get(maxIndex).get("timestampW");
            int maxRTimestamp = (int) versionTable.get(item).get(maxIndex).get("timestampR");
            int maxVersion = (int) versionTable.get(item).get(maxIndex).get("version");

            System.out.println("maxIndex = " + maxIndex + ", maxRTimestamp = " + maxRTimestamp + ", maxWTimestamp = " + maxWTimestamp + ", maxVersion = " + maxVersion);
            
            if (this.transactionCounter[tx] > maxRTimestamp) {
                System.out.println("version_table = " + versionTable.get(item).get(maxIndex));
                // Ganti timestampR menjadi transactionCounter[tx] dan timestampW menjadi maxWTimestamp
                this.versionTable.get(item).get(maxIndex).put("timestampR", this.transactionCounter[tx]);
                this.versionTable.get(item).get(maxIndex).put("timestampW", maxWTimestamp);
            }

            System.out.println("Transaction " + tx + " read " + item + " at version " + maxVersion + ". Timestamp "
                    + item + " now: (" + maxRTimestamp + ", " + maxWTimestamp + ").");

            this.counter++;
        }
    }
    

    public void write(int tx, String item) {
        System.out.println("tx = " + tx + ", item = " + item);
        if (!versionTable.containsKey(item)) {
            System.out.println("kenak 1");
            versionTable.put(item, new ArrayList<>());
            versionTable.get(item).add(Map.of("tx", tx, "timestampR", this.transactionCounter[tx], "timestampW", this.transactionCounter[tx], "version", transactionCounter[tx]));
            sequence.add(Map.of("tx", tx, "item", item, "action", "write", "timestampR", this.transactionCounter[tx], "timestampW", this.transactionCounter[tx], "version", transactionCounter[tx]));
            System.out.println("Transaction " + tx + " wrote " + item + " at version " + this.transactionCounter[tx] + ". Timestamp " + item
                    + " now: (" + this.transactionCounter[tx] + ", " + this.transactionCounter[tx] + ").");
            this.counter++;
        } else {
            int maxIndex = getMaxVersionIndexByWrite(item);
            int maxRTimestamp = (int) versionTable.get(item).get(maxIndex).get("timestampR");
            int maxWTimestamp = (int) versionTable.get(item).get(maxIndex).get("timestampW");
            int maxVersion = (int) versionTable.get(item).get(maxIndex).get("version");

            
            System.out.println("maxIndex = " + maxIndex + ", maxRTimestamp = " + maxRTimestamp + ", maxWTimestamp = " + maxWTimestamp + ", maxVersion = " + maxVersion);
            if (this.transactionCounter[tx] < maxRTimestamp) {
                System.out.println("kenak 2");
                sequence.add(Map.of("tx", tx, "item", null, "action", "write", "timestampR", maxRTimestamp, "timestampW", this.transactionCounter[tx], "version", maxVersion));
                rollback(tx, this.transactionCounter[tx] + 1);
            } else
            if (this.transactionCounter[tx] != maxWTimestamp) {
                System.out.println("kenak 3");
                System.out.println("wakway");
                versionTable.get(item).add(Map.of("tx", tx, "timestampR", maxRTimestamp, "timestampW", this.transactionCounter[tx], "version", transactionCounter[tx]));
                System.out.println("Transaction " + tx + " wrote " + item + " at version " + counter + ". Timestamp " + item
                        + " now: (" + maxRTimestamp + ", " + this.transactionCounter[tx] + ").");
                this.counter++;
            } else {
                System.out.println("kenak 4");
                this.versionTable.get(item).get(maxIndex).putIfAbsent("timestampW", this.transactionCounter[tx]);
                this.versionTable.get(item).get(maxIndex).putIfAbsent("timestampR", maxRTimestamp);
                
                this.sequence.add(Map.of("tx", tx, "item", item, "action", "write", "timestampR", maxRTimestamp, "timestampW", this.transactionCounter[tx], "version", maxVersion));
                this.counter++;
            }   
        }
    }

    private void rollback(int tx, int newTimestamp) {
        List<Map<String, Object>> txSequence = new ArrayList<>();
        for (Map<String, Object> op : sequence) {
            if (((int) op.get("tx")) == tx && !op.get("action").equals("aborted")) {
                txSequence.add(
                        Map.of("tx", op.get("tx"), "item", op.get("item"), "action", op.get("action")));
            }
        }
        for (Map<String, Object> op : inputSequence) {
            if (((int) op.get("tx")) == tx) {
                txSequence.add(op);
                inputSequence.remove(op);
            }
        }
        inputSequence.addAll(txSequence);
        sequence.add(Map.of("tx", tx, "item", null, "action", "rollback"));
        transactionCounter[tx] = counter;
        System.out.println("Transaction " + tx + " rolled back. Assigned new timestamp: " + transactionCounter[tx]
                + ".");
    }

    public void printSequence() {
        for (Map<String, Object> op : sequence) {
            if (!op.get("action").equals("aborted")) {
                System.out.println(op.get("item") + " " + op.get("tx") + " " + op.get("timestampR") + " "
                        + op.get("version"));
            }
        }
    }

    public void run() {
        
        while (!inputSequence.isEmpty()) {
            Map<String, Object> current = inputSequence.poll();
            if (current.get("action").equals("read")) {
                read((int) current.get("tx"), (String) current.get("item"));
            } else if (current.get("action").equals("write")) {
                write((int) current.get("tx"), (String) current.get("item"));
            } else {
                System.out.println("Invalid action.");
            }
            // System.out.println("Version Table: " + versionTable);
            System.out.println();
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter input string (delimited by ;): ");
        String inputString = scanner.nextLine();
        List<Map<String, Object>> inputList = new ArrayList<>();

        String[] inputArray = inputString.split(";");
        System.out.println(Arrays.toString(inputArray));
        for (String input : inputArray) {
            input = input.trim();
            if (!input.equals("")) {
                try {
                    if (input.charAt(0) == 'R') {
                        inputList.add(Map.of("action", "read", "tx", Integer.parseInt(String.valueOf(input.charAt(1))),
                                "item", String.valueOf(input.charAt(3))));
                    } else if (input.charAt(0) == 'W') {
                        inputList.add(Map.of("action", "write", "tx", Integer.parseInt(String.valueOf(input.charAt(1))),
                                "item", String.valueOf(input.charAt(3))));
                    }
                } catch (Exception e) {
                    System.out.println("Invalid input string");
                    System.exit(1);
                }
            }
        }

        MVCC mvcc = new MVCC(inputList);
        mvcc.run();
        mvcc.printSequence();

        scanner.close();
    }
}
