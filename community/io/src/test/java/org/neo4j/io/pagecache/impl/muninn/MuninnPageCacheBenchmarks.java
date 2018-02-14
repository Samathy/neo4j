package org.neo4j.io.pagecache.impl.muninn;


import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.stress.Condition;
import org.neo4j.io.pagecache.stress.PageCacheStressTest;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.io.pagecache.stress.Conditions.numberOfEvictions;

/** Run various runs of the PageCacheStresser in an effort
 * to gather meaningful statistics about the performance of the page
 * cache eviction algorithm.
 *
 * After each test, print the available statistics.
 *
 */
public class MuninnPageCacheBenchmarks {

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    /** This is an absurdly small number of pages.
     * It'll probably fail. **/
    public void stressWithFiftyCachePages() throws Exception {
        DefaultPageCacheTracer monitor;
        monitor = stressDefNoPages(50);
        System.out.println("stressWithFiftyCachePages:");
        printMonitorStats(monitor) ;
        printEightyChars("-");
    }

    @Test
    public void stressWithOneHundredCachePages() throws Exception {
        DefaultPageCacheTracer monitor;
        monitor = stressDefNoPages(100);
        System.out.println("stressWithOneHundredCachePages:");
        printMonitorStats(monitor) ;
        printEightyChars("-");
    }

    @Test
    public void stressWithFiveHundredCachePages() throws Exception {
        DefaultPageCacheTracer monitor;
        monitor = stressDefNoPages(500);
        System.out.println("stressWithFiveHundredCachePages:");
        printMonitorStats(monitor) ;
        printEightyChars("-");
    }

    @Test
    public void stressWithOneThousandCachePages() throws Exception {
        DefaultPageCacheTracer monitor;
        monitor = stressDefNoPages(1000);
        System.out.println("stressWithOneThousandCachePages:");
        printMonitorStats(monitor) ;
        printEightyChars("-");
    }


    /** Run a stress test using a given number of pages
     *
     * @param pages
     * @return
     */
    private DefaultPageCacheTracer stressDefNoPages(int pages) throws Exception
    {
        DefaultPageCacheTracer monitor = new DefaultPageCacheTracer();
        Condition condition = numberOfEvictions(monitor, 100_000);

        PageCacheStressTest runner = new PageCacheStressTest.Builder()
                .withWorkingDirectory(testDirectory.directory())
                .withNumberOfCachePages(pages)
                .with(monitor)
                .with(condition)
                .build();

        runner.run();
        return monitor;
    }

    /** Prints the statistics stored in the PageCacheCounters object
     *
     * @param monitor
     */
    private void printMonitorStats(DefaultPageCacheTracer monitor)
    {
        String tabs = "    ";
        System.out.println(tabs+"Files Mapped:"+monitor.filesMapped());
        System.out.println(tabs+"Faults:"+monitor.faults());
        System.out.println(tabs+"Evictions:"+monitor.evictions());
        System.out.println(tabs+"Pins:"+monitor.pins());
        System.out.println(tabs+"Unpins:"+monitor.unpins());
        System.out.println(tabs+"Hits:"+monitor.hits());
        System.out.println(tabs+"Hit Ratio:"+monitor.hitRatio());
        System.out.println(tabs+"Usage Ratio:"+monitor.usageRatio());
        System.out.println(tabs+"Flushes:"+monitor.flushes());
        System.out.println(tabs+"bytesWritten: "+monitor.bytesWritten());
        System.out.println(tabs+"bytesRead: "+monitor.bytesRead());
    }

    void printEightyChars(String c)
    {
        for (int i = 0; i < 80; i++)
        {
            System.out.print(c);
        }
    }

}
