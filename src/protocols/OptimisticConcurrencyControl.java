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

    /*
     * Validate a given transaction j against other transaction i
     * @param j, transaction id that is about to be committed
     */
    private boolean validateTransaction(String j) {
        boolean returnVal = true;
        Set<String> readSetJ = readSets.get(j);
        for (String i : this.startTimestamp.keySet()) {
            if (!i.equals(j)) {
                if(this.validationTimestamp.get(i) < this.validationTimestamp.get(j)){
                    if (this.finishTimestamp.get(i) < this.startTimestamp.get(j)) {
                        /* Pass */
                    } else if (this.startTimestamp.get(j) < this.finishTimestamp.get(i) && this.finishTimestamp.get(i) < this.validationTimestamp.get(j)) {
                        Set<String> writeSetI = writeSets.get(i);
                        if (!Collections.disjoint(writeSetI, readSetJ)) {
                            returnVal = false;
                            break;
                        }
                    } else {
                        returnVal = false;
                        break;
                    }
                }
            }
        }
        return returnVal;
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
                        System.out.println("READ OF " + matcher.group(3) + " BY T" + matcher.group(2));
                        this.writeSets.get(transactionId).add(matcher.group(3));
                    }else if(matcher.group(1).equals("R")){
                        System.out.println("LOCAL WRITE OF " + matcher.group(3) + " BY T" + matcher.group(2));
                        this.readSets.get(transactionId).add(matcher.group(3));
                    }
                    this.finalSchedule.add(op);
                } else if (matcher.group(4) != null) {
                    String transactionId = matcher.group(5);
                    this.validationTimestamp.put(transactionId, timestamp);
                    if (!validateTransaction(transactionId)) {
                        /* ROLL BACK ALL THE TRANSACTION FOR THE ID */
                        System.out.println("ABORTING T" + transactionId);
                        this.finalSchedule.add("A" + transactionId);

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
                        timestamp++;
                        this.startTimestamp.put(transactionId, timestamp);
                    } else {
                        timestamp++;
                        System.out.println("COMMITTING T"+transactionId + ". WRITING LOCAL WRITE TO DB");
                        this.finalSchedule.add(op);
                        this.finishTimestamp.put(transactionId, timestamp);
                    }
                }
            }
        }

        /* Print the full schedule */
        System.out.println("=================== ");
        System.out.println("Final Schedule: ");
        for (String operation : finalSchedule) {
            System.out.print(operation + " ");
        }
    }

    public static void main(String[] args) {
        System.out.println("Masukkan schedule (diakhiri dengan ;) : ");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();

        OptimisticConcurrencyControl scheduler = new OptimisticConcurrencyControl(input);
        scheduler.scheduler();

        scanner.close();

        // Contoh : R1(A);R2(A);W1(A);W2(A);C1;C2;
    }
}