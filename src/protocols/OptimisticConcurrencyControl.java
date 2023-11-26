import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import java.util.Iterator;

import java.util.regex.Matcher;

class OptimisticConcurrencyControl{
    private Map<String, Integer> startTimestamp;
    private Map<String, Integer> validationTimestamp;
    public Map<String, Integer> finishTimestamp;
    public Deque<String> schedule;
    private Deque<String> finalSchedule;
    private Pattern schedulePattern;
    private ArrayList<Deque<String>> doneTransactions;
    private Integer timestamp;

    public OptimisticConcurrencyControl(String schedule){
        this.startTimestamp = new HashMap<>();
        this.validationTimestamp = new HashMap<>();
        this.finishTimestamp = new HashMap<>();
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.finalSchedule = new ArrayDeque<>();
        this.doneTransactions = new ArrayList<>();
        this.timestamp = 0;

        this.schedule = new ArrayDeque<>(Arrays.asList(schedule.split(  ";")));

        /* Foreach unique object in schedule, initialize the timestamp as 0 */
        for (String operation : this.schedule) {
            Matcher matcher = schedulePattern.matcher(operation);
            if(matcher.matches()){
                if(matcher.group(1) != null){
                    startTimestamp.putIfAbsent(matcher.group(2), 0);
                    validationTimestamp.putIfAbsent(matcher.group(2), 0);
                    finishTimestamp.putIfAbsent(matcher.group(2), 0);
                }else if (matcher.group(4) != null){
                    startTimestamp.putIfAbsent(matcher.group(5), 0);
                    validationTimestamp.putIfAbsent(matcher.group(5), 0);
                    finishTimestamp.putIfAbsent(matcher.group(5), 0);
                }
            }
        }

        for (int i = 0; i < 10; i++){
            doneTransactions.add(new LinkedList<>());
        }
    }

    private boolean validateTransaction(String transactionId) {
        for (Map.Entry<String, Integer> entry : startTimestamp.entrySet()) {
            String otherTransactionId = entry.getKey();
            int otherStartTimestamp = entry.getValue();
    
            if (!transactionId.equals(otherTransactionId)) {
                if (otherStartTimestamp >= this.finishTimestamp.get(transactionId)) {
                    return false;
                }
            }
        }
    
        return true;
    }

    public void scheduler() {
        while(!schedule.isEmpty()){
            timestamp++;
            String op = schedule.poll();
            Matcher matcher = this.schedulePattern.matcher(op);
            if(matcher.matches()){
                /* Set the start timestamp of each transaction */
                if(matcher.group(1) != null){
                    /* Read and Write */
                    if(this.doneTransactions.get(Integer.parseInt(matcher.group(2))).isEmpty()){
                        this.startTimestamp.put(matcher.group(2), timestamp);
                    }
                    this.doneTransactions.get(Integer.parseInt(matcher.group(2))).add(op);
                    this.finalSchedule.add(op);
                }else if (matcher.group(4) != null){
                    this.validationTimestamp.put(matcher.group(5), timestamp);
                    if (!validateTransaction(matcher.group(5))) {
                        /* ROLL BACK ALL THE TRANSACTION FOR THE ID */
                        Iterator<String> it = this.schedule.iterator();
                        while(it.hasNext()){
                            String operation = it.next();
                            if(operation.contains(matcher.group(5))){
                                it.remove();
                                doneTransactions.get(Integer.parseInt(matcher.group(5))).add(operation);
                            }
                        }

                        while(!doneTransactions.get(Integer.parseInt(matcher.group(5))).isEmpty()){
                            String operation = doneTransactions.get(Integer.parseInt(matcher.group(5))).poll();
                            schedule.add(operation);
                        }
                    } else {
                        this.finalSchedule.add(op);
                        this.finishTimestamp.put(matcher.group(5), timestamp);
                    }
                }
            }
        }
        System.out.println("Start Timestamp " + startTimestamp);
        System.out.println("Validation Timestamp " + validationTimestamp);
        System.out.println("Finish Timestamp " + finishTimestamp);
        System.out.println("Done Transactions: " + doneTransactions);
        System.out.println(finalSchedule);
    }

    public static void main(String[] args) {
        OptimisticConcurrencyControl occ = new OptimisticConcurrencyControl("R1(X);W1(X);R2(Y);W2(X);C2;C1");
        occ.scheduler();
    }
}