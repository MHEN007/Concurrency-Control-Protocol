import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class OptimisticConcurrencyControl{
    private Map<String, Integer> startTimestamp;
    private Map<String, Integer> validationTimestamp;
    public Map<String, Integer> finishTimestamp;
    public Deque<String> schedule;
    private Deque<String> finalSchedule;
    private Pattern schedulePattern;

    public OptimisticConcurrencyControl(String schedule){
        this.startTimestamp = new HashMap<>();
        this.validationTimestamp = new HashMap<>();
        this.finishTimestamp = new HashMap<>();
        this.schedulePattern = Pattern.compile("([RW])(\\d+)\\((\\w)\\)|(C)(\\d+)");
        this.finalSchedule = new ArrayDeque<>();

        this.schedule = new ArrayDeque<>(Arrays.asList(schedule.split(",")));

        /* Foreach unique object in schedule, initialize the timestamp as 0 */
        for (String operation : this.schedule) {
            Matcher matcher = schedulePattern.matcher(operation);
            if(matcher.matches()){
                startTimestamp.putIfAbsent(matcher.group(2), 0);
                validationTimestamp.putIfAbsent(matcher.group(2), 0);
                finishTimestamp.putIfAbsent(matcher.group(2), 0);
            }
        }
    }

    public void scheduler() {
        while (!schedule.isEmpty()) {
            String operation = schedule.pollFirst();
            Matcher matcher = schedulePattern.matcher(operation);
            if (matcher.matches()) {
                if (matcher.group(1) != null) {
                    String objectId = matcher.group(2);
                    if (matcher.group(1).equals("R")) {
                        validationTimestamp.putIfAbsent(objectId, 0);
                        validationTimestamp.put(objectId, Math.max(validationTimestamp.get(objectId), startTimestamp.get(objectId)));
                        finalSchedule.add(operation);
                    } else if (matcher.group(1).equals("W")) {
                        // Handle write operation
                        if (validationTimestamp.get(objectId) > startTimestamp.get(objectId)) {
                            // Conflict detected, rollback and retry
                            schedule.addFirst(operation);
                            continue;
                        }
    
                        startTimestamp.put(objectId, finishTimestamp.get(objectId) + 1);
                        finishTimestamp.put(objectId, startTimestamp.get(objectId));
    
                        finalSchedule.add(operation);
                    } else {
                        throw new IllegalArgumentException("Invalid operation type: " + matcher.group(1));
                    }
                } else if (matcher.group(4) != null) {
                    // Handle commit operation
                    String objectId = matcher.group(5);
                    finishTimestamp.put(objectId, startTimestamp.get(objectId));
    
                    // Add operation to final schedule
                    finalSchedule.add(operation);
                }
            } else {
                throw new IllegalArgumentException("Invalid operation: " + operation);
            }
        }
        System.out.println(finalSchedule);
    }

    public static void main(String[] args) {
        OptimisticConcurrencyControl occ = new OptimisticConcurrencyControl("R1(X),W1(Y),R2(Y),W2(Y)");
        occ.scheduler();
    }
}