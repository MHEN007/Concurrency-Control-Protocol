import java.util.*;
import java.util.regex.*;

public class MVCC {
    private Map<String, List<Map<String, Object>>> versionMap;
    private Deque<String> schedule;
    private Deque<String> finalSchedule;
    private Deque<Map<String, Object>> sequence;
    private Pattern schedulePattern;
    private Integer counter;
    private int[] transactionCounter = new int[10];

    public MVCC(String schedule) {
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.finalSchedule = new ArrayDeque<>();
        this.versionMap = new HashMap<>();
        this.counter = 0;
        for (int i = 0; i < transactionCounter.length; i++) {
            this.transactionCounter[i] = i;
        }
        this.schedule = new ArrayDeque<>(Arrays.asList(schedule.split(";")));
        this.sequence = new ArrayDeque<>();
    }

    public void scheduler() {
        while (!schedule.isEmpty()) {
            String op = schedule.poll();
            Matcher matcher = this.schedulePattern.matcher(op);
            if (matcher.matches()) {
                if (matcher.group(1) != null) {
                    String transaction = matcher.group(2);
                    String object = matcher.group(3);
                    if (matcher.group(1).equals("R")) {
                        read(op, transaction, object);
                    } else if (matcher.group(1).equals("W")) {
                        write(op, transaction, object);
                    }
                } else if (matcher.group(4) != null) {
                    /* Commit */
                }
            }
        }
    }

    public void read(String op, String transaction, String object) {
        if (!this.versionMap.containsKey(object)) {
            this.versionMap.put(object, new ArrayList<>());
        }

        int maxIndex = this.getMaxVersionWrite(object);

        if (maxIndex >= this.versionMap.get(object).size()) {
            this.versionMap.get(object).add(new HashMap<>());
        }

        int maxWrite = (int) this.versionMap.get(object).get(maxIndex).getOrDefault("timestampW", 0);
        int maxRead = (int) this.versionMap.get(object).get(maxIndex).getOrDefault("timestampR", 0);
        int maxVersion = (int) this.versionMap.get(object).get(maxIndex).getOrDefault("version", 0);
        HashMap<String, Object> entry = new HashMap<>();
        entry.put("transaction", transaction);
        entry.put("object", object);
        entry.put("op", op);
        entry.put("timestamp", new int[]{maxRead, this.transactionCounter[Integer.parseInt(transaction)]});
        entry.put("version", 0);
        this.sequence.add(entry);

        if (this.transactionCounter[Integer.parseInt(transaction)] > maxRead) {
            this.versionMap.get(object).get(maxIndex).put("timestampR", this.transactionCounter[Integer.parseInt(transaction)]);
            this.versionMap.get(object).get(maxIndex).put("timestampW", maxWrite);
        }

        System.out.println(op + " Read " + object + " at version " + maxVersion + " Timestamp " + object +
                " now is (" + this.versionMap.get(object).get(maxIndex).get("timestampR") + ", " + this.versionMap.get(object).get(maxIndex).get("timestampW")+ ")");

        this.finalSchedule.add(op);
        this.counter++;
    }

    public void write(String op, String transaction, String object) {
        if (!this.versionMap.containsKey(object)) {
            this.versionMap.put(object, new ArrayList<>());
        }

        int maxIndex = this.getMaxVersionWrite(object);

        if (maxIndex >= this.versionMap.get(object).size()) {
            this.versionMap.get(object).add(new HashMap<>());
        }

        int maxWrite = (int) this.versionMap.get(object).get(maxIndex).getOrDefault("timestampW", 0);
        int maxRead = (int) this.versionMap.get(object).get(maxIndex).getOrDefault("timestampR", 0);
        int maxVersion = (int) this.versionMap.get(object).get(maxIndex).getOrDefault("version", 0);

        if (this.transactionCounter[Integer.parseInt(transaction)] < maxRead) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("transaction", transaction);
            entry.put("object", object);
            entry.put("op", op);
            entry.put("timestamp", new int[]{maxRead, this.transactionCounter[Integer.parseInt(transaction)]});
            entry.put("version", maxVersion);
            this.sequence.add(entry);
            this.rollback(transaction);
        } else if (this.transactionCounter[Integer.parseInt(transaction)] < maxWrite) {
            this.versionMap.get(object).get(maxIndex).put("timestampW", this.transactionCounter[Integer.parseInt(transaction)]);
            this.versionMap.get(object).get(maxIndex).put("timestampR", maxRead);
            Map<String, Object> entry = new HashMap<>();
            entry.put("transaction", transaction);
            entry.put("object", object);
            entry.put("op", op);
            entry.put("timestamp", new int[]{maxRead, this.transactionCounter[Integer.parseInt(transaction)]});
            entry.put("version", maxVersion);
            this.sequence.add(entry);
            this.counter++;
        } else {
            this.versionMap.get(object).get(maxIndex).put("timestampW", this.transactionCounter[Integer.parseInt(transaction)]);
            this.versionMap.get(object).get(maxIndex).put("timestampR", maxRead);
            this.versionMap.get(object).get(maxIndex).put("version", this.transactionCounter[Integer.parseInt(transaction)]);
            System.out.println(op + " Write " + object + " at version " + this.transactionCounter[Integer.parseInt(transaction)] + " Timestamp " + object + " now is (" + maxRead + ", " + this.transactionCounter[Integer.parseInt(transaction)] + ")");
            this.finalSchedule.add(op);
            this.counter++;
        }
    }

    public int getMaxVersionWrite(String object) {
        int maxWTimestamp = 0;
        int maxIndex = 0;

        for (int i = 0; i < this.versionMap.get(object).size(); i++) {
            int timestampW = (int) this.versionMap.get(object).get(i).getOrDefault("timestampW", 0);
            if (timestampW > maxWTimestamp) {
                maxWTimestamp = timestampW;
                maxIndex = i;
            }
        }

        return maxIndex;
    }

    public void rollback(String transactionId) {
        List<Map<String, Object>> txSequence = new ArrayList<>();
        for (Map<String, Object> entry : this.sequence) {
            if (entry.get("transaction").equals(transactionId)) {
                txSequence.add(entry);
                this.schedule.remove(entry.get("op"));
            }
        }
        for (Map<String, Object> entryy : txSequence) {
            this.schedule.addLast((String) entryy.get("op"));
        }
        this.transactionCounter[Integer.parseInt(transactionId)] = this.counter;
        System.out.println("Transaction " + transactionId + " rolled back. Assigned new timestamp: " + this.transactionCounter[Integer.parseInt(transactionId)] + ".");
    }

    public static void main(String[] args) {
        MVCC mvcc = new MVCC("R1(X);W3(X);R2(Y);W2(Y);W1(Y);W1(X);C1;C2;C3");
        mvcc.scheduler();
    }
}