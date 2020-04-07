package qp.optimizer;

import qp.operators.Debug;
import qp.operators.Operator;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

import java.util.*;

public class CuckooTabuOptimizer extends RandomOptimizer {
    private final RandomInitialPlan rip;
    private final int INITIAL_EGG_COUNT;
    private final int NUM_ITER;
    private final int MAX_TABU_VALUE;
    private final int MAX_TABU_SIZE;
    private final static PlanCost pc = new PlanCost();

    public CuckooTabuOptimizer(SQLQuery sqlquery) {
        super(sqlquery);
        rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();
        NUM_ITER = 10 * numJoin;
        // values taken from paper, can vary by implementation
        INITIAL_EGG_COUNT = 3;
        MAX_TABU_VALUE = 2;
        MAX_TABU_SIZE = 4;
    }

    @Override
    public Operator getOptimizedPlan() {
        if (numJoin == 0) {
            Operator ip = rip.prepareInitialPlan();
            modifySchema(ip);
            System.out.println("------------ no joins ------------");
            Debug.PPrint(ip);
            System.out.println(pc.getCost(ip));
            return ip;
        }

        // fill up set of initial eggs
        Set<Operator> eggs = new HashSet<>();
        while (eggs.size() < INITIAL_EGG_COUNT) {
            Operator initEgg = rip.prepareInitialPlan();
            modifySchema(initEgg);
            eggs.add(initEgg);
        }
        System.out.println("------------ set of initial random plans ------------");
        eggs.forEach(CuckooTabuOptimizer::show);
        System.out.println("-----------------------------------------------------");

        int tb = 0; // how many consecutive new eggs cannot replace a randomly selected one?
        Deque<Operator> tabu = new ArrayDeque<>(MAX_TABU_SIZE);    // recently laid eggs

        // Cuckoo-tabu hybrid algo
        Operator m = bestEggIn(eggs);
        for (int iter = 0; iter < NUM_ITER; iter++) {
            // generate new egg from a initial egg using P_1
            System.out.println("--------- from m --------------");
            show(m);
            Operator e = step(m);
            System.out.println("--------- generated new egg e --------------");
            show(e);
            // select egg randomly
            Operator r = randomElementFrom(eggs);
            System.out.println("--------- randomly chose r --------------");
            show(r);
            if (pc.getCost(e) < pc.getCost(r)) {
                System.out.println("--------- which got replaced --------------");
                tb = 0;
                // replace r with e
                eggs.remove(r);
                eggs.add(e);
                // remove head from tabu list if full and
                if (tabu.size() == MAX_TABU_SIZE)
                    tabu.pop();
                // add e to tabu
                tabu.push(e);
            } else {
                tb++;
            }
            // stuck with local min, do Lévy flight to try to get out
            if (tb >= MAX_TABU_VALUE || tabu.stream().filter(e::equals).count() >= 2) {
                Operator s = levyFlight(m);
                show(s);
                eggs.add(s);
            }

            m = bestEggIn(eggs);
        }

        System.out.println("------------ left with these plans ------------");
        eggs.forEach(CuckooTabuOptimizer::show);
        System.out.println("---------------------------Final Plan----------------");
        show(m);
        return m;
    }

    /**
     * A single discrete step in the random walk.
     * This implements P_1 in paper.
     */
    private Operator step(Operator o) {
        Operator e;
        do {
            e = getNeighbor((Operator) o.clone());
        } while (o.equals(e)); // avoid the no-op case in neighborAssoc
        return e;
    }

    /**
     * A random step whose distance is heavy-tailed.
     * We iterate neighbour a not-low number of times. This should work when numJoins is not too small, as the graph of
     * query plans connected by the neighbour relation will not be very well-connected, in the sense that when a and b
     * are neighbours, most neighbours of b will be distance 2 from a.
     */
    private Operator levyFlight(Operator o) {
        int iters = RandNumb.randInt(Math.max(numJoin, 4), Math.max(10, 4*numJoin));
        System.out.printf("---------------- Lévy flight, %d steps ----------------\n", iters);
        while (iters-- > 0)
            o = getNeighbor((Operator) o.clone());
        return o;
    }

    private static Operator bestEggIn(Collection<Operator> eggs) {
        long minCost = Long.MAX_VALUE;
        // let's just break ties randomly for additional perturbation
        List<Operator> os = new ArrayList<>();
        for (Operator o : eggs) {
            long cost = pc.getCost(o);
            if (cost <= minCost) {
                if (cost < minCost)
                    os.clear();
                os.add(o);
                minCost = cost;
            }
        }
        return randomElementFrom(os);
    }

    private static <T> T randomElementFrom(Collection<T> c) {
        int rand = RandNumb.randInt(0, c.size()-1);
        for (T x: c)
            if (rand-- == 0)
                return x;

        System.err.println("something wrong with rand?");
        System.exit(1);
        return null;
    }

    private static void show(Operator o) {
        Debug.PPrint(o);
        System.out.println(pc.getCost(o));
    }
}
