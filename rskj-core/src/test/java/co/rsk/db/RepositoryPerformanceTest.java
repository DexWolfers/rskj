package co.rsk.db;

import co.rsk.config.RskSystemProperties;
import co.rsk.config.TestSystemProperties;
import co.rsk.core.RskAddress;
import co.rsk.helpers.PerformanceTestHelper;
import co.rsk.trie.TrieStore;
import org.bouncycastle.util.encoders.Hex;
import org.ethereum.TestUtils;
import org.ethereum.core.Repository;
import org.ethereum.db.ContractDetails;
import org.ethereum.vm.DataWord;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by SerAdmin on 10/22/2018.
 */
public class RepositoryPerformanceTest {
    private final TestSystemProperties config = new TestSystemProperties();

    @Ignore
    @Test
    public void testAccountCreation() {
        PerformanceTestHelper pth = new PerformanceTestHelper();

        int createCount = 1000*1000;
        Repository repository = createRepositoryImpl(config,true);
        Repository track = repository.startTracking();
        pth.setup();

        pth.startMeasure();

        for(int t=0;t<createCount;t++) {
            RskAddress addr = TestUtils.randomAddress();
            track.createAccount(addr);
        }
        pth.endMeasure("Accounts added"); // partial result
        track.commit();
        pth.endMeasure("Accounts committed"); // final result

    }

    @Ignore
    @Test
    public void testStorageRowsCreation() {
        PerformanceTestHelper pth = new PerformanceTestHelper();

        int createCount = 1000*1000;
        Repository repository = createRepositoryImpl(config,true);
        Repository track = repository.startTracking();
        pth.setup();

        pth.startMeasure();

        RskAddress addr = TestUtils.randomAddress();

        track.createAccount(addr);
        track.setupContract(addr);

        for(int t=0;t<createCount;t++) {
            track.addStorageRow(addr,TestUtils.randomDataWord(),TestUtils.randomDataWord());
        }

        pth.endMeasure("Storage rows added"); // partial result
        track.commit();
        pth.endMeasure("Storage rows committed"); // final result

    }

    public static RepositoryImpl createRepositoryImpl(RskSystemProperties config, boolean isSecure) {
        return new RepositoryImpl(isSecure);
    }
}
