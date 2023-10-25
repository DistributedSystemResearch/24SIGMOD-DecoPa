package com.huberlin.util;
import java.util.HashMap;
import java.util.Set;

public class BijectiveMap<X, Y> {
    private final HashMap<X,Y> f = new HashMap<>();
    private final HashMap<Y, X> fminus1 = new HashMap<>();

    public Y get(X x) {
        return this.f.get(x);
    }

    public X getKey(Y y) {
        return this.fminus1.get(y);
    }

    /**
     * Return set of X values. Do not modify this set (it will break the invariant)
     * @return
     */
    public Set<X> keySet() {
        return this.f.keySet();
    }

    public void put(X x, Y y) {
        if (this.f.containsKey(x)) {
            Y old_y = this.f.get(x);
            this.fminus1.remove(old_y);
        }
        this.fminus1.put(y, x);
        this.f.put(x,y);
    }
}

