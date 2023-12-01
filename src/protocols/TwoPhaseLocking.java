import java.util.*;
import java.util.regex.*;

class TwoPhaseLocking{
    private ArrayList<String> startTime;
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
        this.schedule = new ArrayDeque<>(Arrays.asList(schedule.split(";")));
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.lockTable = new LinkedList<>();
        this.finalSchedule = new LinkedList<>();
        this.waitQueue = new LinkedList<>();
        this.doneTransactions = new ArrayList<>();
        this.startTime = new ArrayList<>();

        /* Foreach unique object in schedule, initialize the timestamp as 0 */
        for (String operation : this.schedule) {
            Matcher matcher = schedulePattern.matcher(operation);
            if(matcher.matches()){
                if(matcher.group(2) != null){
                    if(!startTime.contains(matcher.group(2))){
                        startTime.add(matcher.group(2));
                    }
                }else if(matcher.group(5) != null){
                    if(!startTime.contains(matcher.group(5))){
                        startTime.add(matcher.group(5));
                    }
                }
            }
        }

        for (int i = 0; i < 100; i++){
            this.doneTransactions.add(new LinkedList<>());
        }
    }

    /* 
     * Checker for validation if a lock is to be given or not
     * @param operation: Operation name
     * @returns boolean: true if the lock is to be given and false if there is conflict
     */
    private boolean exclusiveLockChecker(String operation, String object, String id) {
        if(operation.equals("R")){
            /* Check if there is an exclusive lock on the object from the same Ti*/
            if(lockTable.contains("XL" + id + "(" + object + ")")){
                return true;
            }else{ 
                /* check if there exists a exclusive lock on the object */  
                Boolean check1 = lockTable.stream().anyMatch(lock -> lock.endsWith("("+object+")") && lock.startsWith("XL"));
                return check1 ? false : true;
            }
        }else if(operation.equals("W")){
            /* Check if there are any exclusive locks on the object from the same Ti*/
            if(lockTable.contains("XL" + id + "(" + object + ")")){
                return true;
            }else{
                /* Check if there exists a exclusive lock on the object*/
                if(lockTable.stream().anyMatch(lock -> lock.endsWith("("+object+")") && lock.startsWith("XL"))){
                    return false;
                }else{
                    /* check if there exists a share lock where the id is the same */
                    long sharedLockCount = lockTable.stream()
                                            .filter(lock -> lock.startsWith("SL") && lock.endsWith("(" + object + ")"))
                                            .count();
                    if(lockTable.contains("SL"+id+"("+object+")") && sharedLockCount == 1){
                        return true;
                    }else{
                        if(sharedLockCount < 1){
                            return true;
                        }else{
                            return false;
                        }
                    }
                }
            }
        }else{
            return true;
        }
    }

    /*
     * Returns the ID of the owner of the lock on the object
     */
    private static List<Integer> getTransactionLockIds(Deque<String> deque, String object) {
        List<Integer> lockIds = new ArrayList<>();
    
        for (String lock : deque) {
            if (lock.endsWith("(" + object + ")")) {
                // Extract the number from the lock
                int startIndex = lock.indexOf('L') + 1;  // Skip the 'L' character
                int endIndex = lock.indexOf('(');
                String numberStr = lock.substring(startIndex, endIndex);
                int lockId = Integer.parseInt(numberStr);
                lockIds.add(lockId);
            }
        }
    
        return lockIds;
    }

    public boolean isCurrentTransactionYounger(String id, String object) {
        List<Integer> lockIds = getTransactionLockIds(this.lockTable, object);
        return lockIds.stream().anyMatch(tranId -> this.startTime.indexOf(id) > this.startTime.indexOf(Integer.toString(tranId)));  
    }

    /*
     * Removes all locks that transaction-id has
     * @param id: String
     */
    private void releaseLocks(String id){
        /* Remove all locks on lockTable and sharedLocks (both are queue with formats XLi(X) or SLi(X))*/
        lockTable.removeIf(lock -> lock.startsWith("XL"+id));
        lockTable.removeIf(lock -> lock.startsWith("SL"+id));
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
                    String check = this.waitQueue.stream().filter(wait -> wait.contains(matcher.group(2))).findFirst().orElse("");
                    Boolean isNotWaiting = true;
                    if(!check.isEmpty()){
                        Matcher m1 = this.schedulePattern.matcher(check);
                        if(m1.matches()){
                            if(m1.group(1) != null)
                                isNotWaiting = this.exclusiveLockChecker(m1.group(1), m1.group(3), m1.group(2));
                        }
                    }
                    if(isNotWaiting){
                        if(matcher.group(1).equals("R")){
                            /* Check if there is an shared lock on this object
                            * If there is and belongs to this transaction, proceed
                            * If there is none, add the lock and proceed
                            * If exists but doesn't belong to this transaction, give the lock to this transaction
                            * If exists a exclusive lock, wait
                            */
                            if(this.exclusiveLockChecker(matcher.group(1), matcher.group(3), matcher.group(2))){
                                doneTransactions.get(Integer.parseInt(matcher.group(2))).add(operation);
                                if(!lockTable.contains("SL"+matcher.group(2)+"("+matcher.group(3)+")")){
                                    lockTable.add("SL"+matcher.group(2)+"("+matcher.group(3)+")");
                                    finalSchedule.add("SL"+matcher.group(2)+"("+matcher.group(3)+")");
                                }
                                finalSchedule.add(operation);
                            }else{
                                // Deadlock prevention using Wait and Die scheme
                                if(isCurrentTransactionYounger(matcher.group(2), matcher.group(3))){
                                    /* Rollback */
                                    finalSchedule.add("Aborting transaction " + matcher.group(2));
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

                                    while(!doneTransactions.get(Integer.parseInt(matcher.group(2))).isEmpty()){
                                        String op = doneTransactions.get(Integer.parseInt(matcher.group(2))).poll();
                                        schedule.add(op);
                                    }

                                    startTime.removeIf(id->id.equals(matcher.group(2)));
                                    startTime.add(matcher.group(2));
                                    
                                } else {
                                    /* Waiting */
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
                                doneTransactions.get(Integer.parseInt(matcher.group(2))).add(operation);
                                if(!lockTable.contains("XL"+matcher.group(2)+"("+matcher.group(3)+")")){
                                    lockTable.add("XL"+matcher.group(2)+"("+matcher.group(3)+")");
                                    finalSchedule.add("XL"+matcher.group(2)+"("+matcher.group(3)+")");
                                }
                                finalSchedule.add(operation);
                            }else{
                                if(isCurrentTransactionYounger(matcher.group(2), matcher.group(3))){
                                    /* Rollback */
                                    finalSchedule.add("Aborting transaction " + matcher.group(2));
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

                                    while(!doneTransactions.get(Integer.parseInt(matcher.group(2))).isEmpty()){
                                        String op = doneTransactions.get(Integer.parseInt(matcher.group(2))).poll();
                                        schedule.add(op);
                                    }

                                    startTime.removeIf(id->id.equals(matcher.group(2)));
                                    startTime.add(matcher.group(2));

                                } else {
                                    /* Waiting */
                                    finalSchedule.add("Transaction " + matcher.group(2) + " waits for lock");
                                    waitQueue.add(operation);
                                }
                            }
                        }
                    }else{
                        waitQueue.add(operation);
                    }
                }else if(matcher.group(4) != null){
                    // System.out.println("Wait " + waitQueue);
                    if(waitQueue.stream().anyMatch(lock -> lock.contains(matcher.group(5)))){
                        waitQueue.add(operation);
                    }else{
                        finalSchedule.add(operation);
                        releaseLocks(matcher.group(5));
                        doneTransactions.get(Integer.parseInt(matcher.group(5))).add(operation);
                    }
                }  
            }

            if(!this.waitQueue.isEmpty()){
                Matcher matcher1 = this.schedulePattern.matcher(waitQueue.peek());
                if(matcher1.matches()){
                    /* If the exclusiveLock check is free then insert to the left of the queue */
                    if(matcher1.group(1) != null){
                        if(exclusiveLockChecker(matcher1.group(1), matcher1.group(3), matcher1.group(2))){
                            schedule.addFirst(waitQueue.poll());
                        }
                    }else if(matcher1.group(4) != null){
                        schedule.addFirst(waitQueue.poll());
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
        System.out.println("Masukkan schedule (diakhiri dengan ;) : ");
        Scanner scanner = new Scanner(System.in);

        TwoPhaseLocking twoPhaseLocking = new TwoPhaseLocking(scanner.nextLine());
        twoPhaseLocking.scheduler();

        scanner.close();

        // R1(X);W2(X);W2(Y);W3(Y);W1(X);C1;C2;C3
    }
}