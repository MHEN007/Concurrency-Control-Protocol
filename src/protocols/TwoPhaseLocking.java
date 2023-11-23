import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.*;

class TwoPhaseLocking{
    public ArrayList<String> schedule;
    public Queue<String> sharedLocks; /* Format SLi(X) */
    public Queue<String> exclusiveLocks; /* Format XLi(X) */
    public Queue<String> waitQueue;
    public Queue<String> finalSchedule;
    private Pattern schedulePattern;

    /* 
     * Class constructor
     * @param schedule Format as follows: Ri(X),Wi(X),Ci
     */
    public TwoPhaseLocking(String schedule){
        this.schedule = new ArrayList<>(Arrays.asList(schedule.split(",")));
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.sharedLocks = new LinkedList<>();
        this.exclusiveLocks = new LinkedList<>();
        this.finalSchedule = new LinkedList<>();
        this.waitQueue = new LinkedList<>();
    }

    private boolean sharedLockChecker(String object, String id) {
        if(exclusiveLocks.isEmpty()){
            if(sharedLocks.isEmpty()){
                return true;
            }else{
                /* Check if there exists SL with the same id on the same object
                 * If exists, return true
                 * If doesn't exists, check if there is a lock on the object
                 */
                if(sharedLocks.stream().anyMatch(lock -> lock.startsWith("SL" + id + "(" + object + ")"))){
                    return true;
                } else if(sharedLocks.stream().anyMatch(lock -> lock.endsWith("(" + object + ")"))) {
                    return true;
                } else {
                    return true;
                }
            }
        }else{
            // TODO: IMPLEMENT
            return false;
        }
    }

    private boolean exclusiveLockChecker(String object, String id) {
        /* Check if on this transaction holds a SL on object */
        if(sharedLocks.contains("SL"+id+"("+object+")")){
            /* Upgrade the lock 
             * remove the object from sharelock
             * return true
            */
            sharedLocks.remove("SL"+id+"("+object+")");
            return true;
        }else{
            if(exclusiveLocks.contains("XL"+id+"("+object+")")){
                return true;
            }else{
                return false;
            }
        }
    }

    private void releaseLocks(String id){
        /* Remove all locks on exclusiveLocks and sharedLocks (both are queue with formats XLi(X) or SLi(X))*/
        exclusiveLocks.removeIf(lock -> lock.startsWith("XL"+id));
        sharedLocks.removeIf(lock->lock.startsWith("SL"+id));
    }

    /*
     * Scheduler
     * Schedule the Transaction
     * Schedule using automatic acquisation
     */
    public void scheduler(){
        for (String operation : this.schedule) {
            Matcher matcher = this.schedulePattern.matcher(operation);
            if(matcher.matches()){
                if(matcher.group(1) != null){
                    // System.out.println("Operation: " + matcher.group(1));
                    // System.out.println("Transaction ID: " + matcher.group(2));
                    // System.out.println("Object: " + matcher.group(3));
                    if(matcher.group(1).equals("R")){
                        /* Check if there is an shared lock on this object
                         * If there is and belongs to this transaction, proceed
                         * If there is none, add the lock and proceed
                         * If exists but doesn't belong to this transaction, give the lock to this transaction
                         * If exists a exclusive lock, wait
                         */
                        if(this.sharedLockChecker(matcher.group(3), matcher.group(2))){
                            sharedLocks.add("SL"+matcher.group(2)+"("+matcher.group(3)+")");
                            finalSchedule.add("SL"+matcher.group(2)+"("+matcher.group(3)+")");
                            finalSchedule.add(operation);
                        }else{
                            // WAIT
                        }
                    }else if(matcher.group(1).equals("W")){
                        /* Check in SL if there is already a lock or not. 
                         * If there already exist and is has the same id, remove the lock, upgrade to this lock
                         * If there already exist and doesn't have the same id, wait until it exists
                         * If there is none, proceed
                         */
                        if(this.exclusiveLockChecker(matcher.group(3), matcher.group(2))){
                            exclusiveLocks.add("XL"+matcher.group(2)+"("+matcher.group(3)+")");
                            finalSchedule.add("XL"+matcher.group(2)+"("+matcher.group(3)+")");
                            finalSchedule.add(operation);
                        }else{
                            // WAIT
                        }
                    }

                }else if(matcher.group(4) != null){
                    // System.out.println("Operation: " + matcher.group(4));
                    // System.out.println("Transaction ID: " + matcher.group(5));
                    /* Release all locks on with transaction i */
                    releaseLocks(matcher.group(5));
                    finalSchedule.add(operation);
                }
            }
        }

        /* Print the full schedule */
        for (String operation : finalSchedule) {
            System.out.println(operation);
        }
    }

    public static void main(String[] args) {
        TwoPhaseLocking twoPhaseLocking = new TwoPhaseLocking("R1(X),R2(X),W1(X),W2(X),C1,C2");
        twoPhaseLocking.scheduler();
    }
}