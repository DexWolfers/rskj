/*
 * This file is part of RskJ
 * Copyright (C) 2018 RSK Labs Ltd.
 * (derived from ethereumJ library, Copyright (c) 2016 <ether.camp>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.ethereum.datasource;

import org.ethereum.db.ByteArrayWrapper;

import java.util.*;

public class DataSourceWithCache implements KeyValueDataSource {
    private KeyValueDataSource base;
    private HashMapDB uncommittedCache;
    private HashMapDB committedCache;

    // During the processing of a Raul's fully filled blockchain, the cache
    // is generating the following hits per block (average)
    // uncommitedCacheHits = 1340
    // commitedCacheHits = 248
    // Processing 134 blocks grows the commitedCache to 100K entries, or approximately
    // or 25 Mbytes. A cache of about 100 Mbytes seems rasonable. Anyway, for
    // precaution we'll set the limit to 100K entries.
    public DataSourceWithCache(KeyValueDataSource base, int cacheSize) {
        this.base = base;
        this.uncommittedCache = new HashMapDB();
        this.committedCache = new HashMapDB(cacheSize,true);
    }

    public byte[] get(byte[] key) {
        byte[] r;

        r = committedCache.get(key);
        if (r != null) {
            return r;
        }

        r = uncommittedCache.get(key);
        if (r != null) {
            return r;
        }

        r = base.get(key);
        committedCache.put(key,r);
        return r;
    }

    public byte[] put(byte[] key, byte[] value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        /**/
        // here I could check for equal datas or just move to the uncommited uncommittedCache.
        byte[] priorValue = committedCache.get(key);
        if (priorValue != null && Arrays.equals(priorValue, value)) {
            return value;
        }

        committedCache.delete(key);
        return uncommittedCache.put(key,value);
    }

    public void delete(byte[] key) {
        Objects.requireNonNull(key);
        // fully delete this element
        committedCache.delete(key);

        // Here we MUST NOT use delete() because we have to mark that the
        // element should be deleted from the base.
        uncommittedCache.put(key,null);
    }

    private void addKeys(Set<byte[]> result, KeyValueDataSource map) {
        for (byte[] k : map.keys()) {
            if (map.get(k).length != 0) {
                result.add(k);
            } else {
                result.remove(k);
            }
        }
    }

    public Set<byte[]> keys() {
        Set<byte[]> result = new HashSet<>(base.keys());
        addKeys(result, committedCache);
        addKeys(result, uncommittedCache);
        return result;


    }

    public void updateBatch(Map<ByteArrayWrapper, byte[]> rows) {
        // Remove from the commited set all elements in this batch
        committedCache.removeBatch(rows);
        uncommittedCache.updateBatch(rows);
    }

    public synchronized void flush() {
        // commited values need not be re-updated
        base.updateBatch(uncommittedCache.getStorageMap());

        // move all uncommited to commited. There should be no duplicated, by design.
        committedCache.addBatch(uncommittedCache.getStorageMap());
        uncommittedCache.clear();
    }

    public String getName() {
        return base.getName()+"-with-uncommittedCache";
    }

    public void init() {
        base.init();
        uncommittedCache.init();
        committedCache.init();
    }

    public boolean isAlive() {
        return base.isAlive();
    }

    public void close() {
        flush();
        base.close();
        uncommittedCache.close();
        committedCache.close();
    }
}
