import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.*;

class TwoPhaseLocking{
    public Deque<String> schedule;
    public ArrayList<Deque<String>> doneTransactions;
    public Deque<String> lockTable;
    public Deque<String> waitQueue;
    public Deque<String> finalSchedule;
    private Pattern schedulePattern;

    /* 
     * Class constructor
     * @param schedule Format as follows: Ri(X),Wi(X),Ci
     */
    public TwoPhaseLocking(String schedule){
        this.schedule = new ArrayDeque<>(Arrays.asList(schedule.split(",")));
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.lockTable = new LinkedList<>();
        this.finalSchedule = new LinkedList<>();
        this.waitQueue = new LinkedList<>();
        this.doneTransactions = new ArrayList<>();

        for (int i = 0; i < 10; i++){
            this.doneTransactions.add(new LinkedList<>());
        }
    }

    private boolean exclusiveLockChecker(String operation, String object, String id) {
        if(operation.equals("R") || operation.equals("W")){
            /* Check if there is an exclusive lock on the object */
            return lockTable.stream().anyMatch(lock -> lock.endsWith("("+object+")")) ? false : true;
        }else{
            return true;
        }
    }

    private static int getTransactionLockId(Deque<String> deque, String object) {
        for (String lock : deque) {
            if (lock.endsWith("(" + object + ")")) {
                // Extract the number from the lock
                int startIndex = lock.indexOf('L') + 1;  // Skip the 'L' character
                int endIndex = lock.indexOf('(');
                String numberStr = lock.substring(startIndex, endIndex);
                return Integer.parseInt(numberStr);
            }
        }
        return -1; // Object A not found in the deque
    }

    public boolean isCurrentTransactionYounger(String id, String object){
        int tran_id = getTransactionLockId(this.lockTable, object);
        return Integer.parseInt(id) > tran_id ? true : false;
    }

    private void releaseLocks(String id){
        /* Remove all locks on lockTable and sharedLocks (both are queue with formats XLi(X) or SLi(X))*/
        lockTable.removeIf(lock -> lock.startsWith("XL"+id));

        /* Because lock has been removed. Try to execute the waiting queue */

    }

    /*
     * Scheduler
     * Schedule the Transaction
     * Schedule using automatic acquisation
     */
    public void scheduler(){
        while(!schedule.isEmpty()) {
            String operation = schedule.poll();
            Matcher matcher = this.schedulePattern.matcher(operation);
            if(matcher.matches()){
                if(matcher.group(1) != null){
                    if(matcher.group(1).equals("R")){
                        /* Check if there is an shared lock on this object
                         * If there is and belongs to this transaction, proceed
                         * If there is none, add the lock and proceed
                         * If exists but doesn't belong to this transaction, give the lock to this transaction
                         * If exists a exclusive lock, wait
                         */
                        if(this.exclusiveLockChecker(matcher.group(1), matcher.group(3), matcher.group(2))){
                            doneTransactions.get(Integer.parseInt(matcher.group(2))).add(operation);
                            finalSchedule.add("SL"+matcher.group(2)+"("+matcher.group(3)+")");
                            finalSchedule.add(operation);
                        }else{
                            // Deadlock prevention using Wait and Die scheme
                            if(isCurrentTransactionYounger(matcher.group(2), matcher.group(3))){
                                /* Rollback */
                                doneTransactions.get(Integer.parseInt(matcher.group(2))).add(operation);
                                Iterator<String> iterator = schedule.iterator();
                                while (iterator.hasNext()) {
                                    String op = iterator.next();
                                    if (op.contains(matcher.group(2))) {
                                        iterator.remove();
                                        doneTransactions.get(Integer.parseInt(matcher.group(2))).add(op);
                                    }
                                }

                                releaseLocks(matcher.group(2));

                                /* Remove remaining operation in schedule where it contains the number in matcher.group(2) */
                                schedule.removeIf(lock -> lock.contains(matcher.group(2)+"("));

                                while(!doneTransactions.get(Integer.parseInt(matcher.group(2))).isEmpty()){
                                    String op = doneTransactions.get(Integer.parseInt(matcher.group(2))).poll();
                                    schedule.add(op);
                                }
                                
                                finalSchedule.add("Aborting transaction " + matcher.group(2));
                            } else {
                                finalSchedule.add("Transaction " + matcher.group(2) + " waits for lock");
                                waitQueue.add(operation);
                            }
                        }
                    }else if(matcher.group(1).equals("W")){
                        /* Check in SL if there is already a lock or not. 
                         * If there already exist and is has the same id, remove the lock, upgrade to this lock
                         * If there already exist and doesn't have the same id, wait until it exists
                         * If there is none, proceed
                         */
                        if(this.exclusiveLockChecker(matcher.group(1), matcher.group(3), matcher.group(2))){
                            lockTable.add("XL"+matcher.group(2)+"("+matcher.group(3)+")");
                            doneTransactions.get(Integer.parseInt(matcher.group(2))).add(operation);
                            finalSchedule.add("XL"+matcher.group(2)+"("+matcher.group(3)+")");
                            finalSchedule.add(operation);
                        }else{
                            if(isCurrentTransactionYounger(matcher.group(2), matcher.group(3))){
                                /* Rollback */
                                doneTransactions.get(Integer.parseInt(matcher.group(2))).add(operation);
                                Iterator<String> iterator = schedule.iterator();
                                while (iterator.hasNext()) {
                                    String op = iterator.next();
                                    if (op.contains(matcher.group(2))) {
                                        iterator.remove();
                                        waitQueue.add(op);
                                    }
                                }

                                while(!doneTransactions.get(Integer.parseInt(matcher.group(2))).isEmpty()){
                                    String op = doneTransactions.get(Integer.parseInt(matcher.group(2))).poll();
                                    schedule.add(op);
                                }

                                finalSchedule.add("Aborting transaction " + matcher.group(2));
                            } else {
                                /* Waiting */
                                finalSchedule.add("Transaction " + matcher.group(2) + " waits for lock");
                                waitQueue.add(operation);
                                Iterator<String> iterator = schedule.iterator();
                                while(iterator.hasNext()) {
                                    String op = iterator.next();
                                    if (op.contains(matcher.group(2))) {
                                        iterator.remove();
                                        waitQueue.add(op);
                                    }
                                }
                            }
                        }
                    }

                }else if(matcher.group(4) != null){
                    releaseLocks(matcher.group(5));
                    doneTransactions.get(Integer.parseInt(matcher.group(5))).add(operation);
                    finalSchedule.add(operation);
                }
            }
        }

        /* Print the full schedule */
        for (String operation : finalSchedule) {
            System.out.print(operation + "\n");
        }
    }

    public static void main(String[] args) {
        TwoPhaseLocking twoPhaseLocking = new TwoPhaseLocking("R1(X),R2(X),W2(X),W1(X),C1,C2");
        twoPhaseLocking.scheduler();
    }
}