import java.util.*;
import java.util.regex.*;

public class OptimisticConcurrencyControl {
    private Map<String, Integer> startTimestamp;
    private Map<String, Integer> validationTimestamp;
    private Map<String, Integer> finishTimestamp;
    private Deque<String> schedule;
    private Deque<String> finalSchedule;
    private Pattern schedulePattern;
    private ArrayList<Deque<String>> doneTransactions;
    private Map<String, Set<String>> writeSets, readSets;  // Added write sets
    private Integer timestamp;

    public OptimisticConcurrencyControl(String schedule) {
        this.startTimestamp = new HashMap<>();
        this.validationTimestamp = new HashMap<>();
        this.finishTimestamp = new HashMap<>();
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.finalSchedule = new ArrayDeque<>();
        this.doneTransactions = new ArrayList<>();
        this.writeSets = new HashMap<>();
        this.readSets = new HashMap<>();
        this.timestamp = 0;

        this.schedule = new ArrayDeque<>(Arrays.asList(schedule.split(";")));

        /* For each unique object in the schedule, initialize the timestamp as 0 */
        for (String operation : this.schedule) {
            Matcher matcher = schedulePattern.matcher(operation);
            if (matcher.matches()) {
                if (matcher.group(1) != null) {
                    startTimestamp.putIfAbsent(matcher.group(2), 0);
                    validationTimestamp.putIfAbsent(matcher.group(2), 0);
                    finishTimestamp.putIfAbsent(matcher.group(2), 0);
                    writeSets.putIfAbsent(matcher.group(2), new HashSet<>());
                    readSets.putIfAbsent(matcher.group(2), new HashSet<>());
                } else if (matcher.group(4) != null) {
                    startTimestamp.putIfAbsent(matcher.group(5), 0);
                    validationTimestamp.putIfAbsent(matcher.group(5), 0);
                    finishTimestamp.putIfAbsent(matcher.group(5), 0);
                    writeSets.putIfAbsent(matcher.group(5), new HashSet<>());
                    readSets.putIfAbsent(matcher.group(5), new HashSet<>());
                }
            }
        }

        for (int i = 0; i < 10; i++) {
            doneTransactions.add(new LinkedList<>());
        }
    }

    private boolean validateTransaction(String i) {
        Set<String> readSetI = readSets.get(i);
        for (String j : this.startTimestamp.keySet()) {
            if (!j.equals(i)) {
                if (this.finishTimestamp.get(j) < this.startTimestamp.get(i) ||
                        (this.startTimestamp.get(j) < this.finishTimestamp.get(i)
                                && this.finishTimestamp.get(i) < this.validationTimestamp.get(j))) {
                    /* Pass */
                } else {
                    Set<String> writeSetJ = writeSets.get(j);
                    if (!Collections.disjoint(readSetI, writeSetJ)) {
                        return false;  // There is an intersection in the write sets
                    }
                }
            }
        }

        return true;
    }

    public void scheduler() {
        while (!schedule.isEmpty()) {
            timestamp++;
            String op = schedule.poll();
            Matcher matcher = this.schedulePattern.matcher(op);
            if (matcher.matches()) {
                /* Set the start timestamp of each transaction */
                if (matcher.group(1) != null) {
                    /* Read and Write */
                    String transactionId = matcher.group(2);
                    if (this.doneTransactions.get(Integer.parseInt(transactionId)).isEmpty()) {
                        this.startTimestamp.put(transactionId, timestamp);
                    }
                    this.doneTransactions.get(Integer.parseInt(transactionId)).add(op);
                    if(matcher.group(1).equals("W")){
                        this.writeSets.get(transactionId).add(matcher.group(3));
                    }else if(matcher.group(1).equals("R")){
                        this.readSets.get(transactionId).add(matcher.group(3));
                    }
                    this.finalSchedule.add(op);
                } else if (matcher.group(4) != null) {
                    String transactionId = matcher.group(5);
                    this.validationTimestamp.put(transactionId, timestamp);
                    if (!validateTransaction(transactionId)) {
                        /* ROLL BACK ALL THE TRANSACTION FOR THE ID */
                        this.finalSchedule.add("ROLLBACK T" + transactionId);

                        Iterator<String> it = this.schedule.iterator();
                        doneTransactions.get(Integer.parseInt(transactionId)).add(op);
                        while (it.hasNext()) {
                            String operation = it.next();
                            if (operation.contains(transactionId)) {
                                it.remove();
                                doneTransactions.get(Integer.parseInt(transactionId)).add(operation);
                            }
                        }

                        while (!doneTransactions.get(Integer.parseInt(transactionId)).isEmpty()) {
                            String operation = doneTransactions.get(Integer.parseInt(transactionId)).poll();
                            schedule.add(operation);
                        }

                        /* Empty the writeset on a given id */
                        this.writeSets.get(transactionId).clear();
                        this.readSets.get(transactionId).clear();
                    } else {
                        timestamp++;
                        this.finalSchedule.add(op);
                        this.finishTimestamp.put(transactionId, timestamp);
                    }
                }
            }
        }

        /* Print the full schedule */
        for (String operation : finalSchedule) {
            System.out.print(operation + "\n");
        }
    }

    public static void main(String[] args) {
        OptimisticConcurrencyControl occ = new OptimisticConcurrencyControl("R1(a);R2(b);W1(b);W2(a);C1;C2");
        occ.scheduler();
    }
}