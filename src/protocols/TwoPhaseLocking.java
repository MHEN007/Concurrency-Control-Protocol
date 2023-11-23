import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;

class TwoPhaseLocking{
    public ArrayList<String> schedule;
    public Queue<String> shareLocks;
    public Queue<String> exclusiveLocks;
    /* 
     * Class constructor
     * @param schedule Format as follows: Ri(X),Wi(X),Ci(X)
     */
    public TwoPhaseLocking(String schedule){
        this.schedule = new ArrayList<>(Arrays.asList(schedule.split(",")));
    }

    /*
     * Scheduler
     * Schedule the Transaction
     */
    public void scheduler(){

    }

    public static void main(String[] args) {
        TwoPhaseLocking twoPhaseLocking = new TwoPhaseLocking("R1(X),W1(X),C1(X)");
        for (String string : twoPhaseLocking.schedule) {
            System.out.println(string);
        }
    }
}