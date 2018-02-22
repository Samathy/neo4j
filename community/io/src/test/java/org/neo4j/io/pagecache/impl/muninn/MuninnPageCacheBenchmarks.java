package org.neo4j.io.pagecache.impl.muninn;


import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.pagecache.stress.Condition;
import org.neo4j.io.pagecache.stress.PageCacheStressTest;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.test.rule.TestDirectory;

import java.util.Map;

import static org.neo4j.io.pagecache.stress.Conditions.numberOfEvictions;

/** Run various runs of the PageCacheStresser in an effort
 * to gather meaningful statistics about the performance of the page
 * cache eviction algorithm.
 *
 * After each test, print the available statistics.
 *
 */
public class MuninnPageCacheBenchmarks {

    Map<String, DefaultPageCursorTracer> results;
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @BeforeClass
    public static void printHeadings()
    {
        System.out.println("testName, faults,evictions, pins, hits, hitRatio, flushes, bytesWritten, bytesRead");
    }

    @Test
    /** This is an absurdly small number of pages.
     * It'll probably fail. **/
    public void stressWithFiftyCachePages() throws Exception {
        DefaultPageCursorTracer monitor;
        monitor = stressDefNoPages(50);
        printMonitorStats("stressWithFiftyCachePages", monitor);
    }

    @Test
    public void stressWithOneHundredCachePages() throws Exception {
        DefaultPageCursorTracer monitor;
        monitor = stressDefNoPages(100);
        printMonitorStats("stressWithOneHundredCachePages", monitor);
    }

    @Test
    public void stressWithFiveHundredCachePages() throws Exception {
        DefaultPageCursorTracer monitor;
        monitor = stressDefNoPages(500);
        printMonitorStats("stressWithFiveHundredCachePages", monitor);
    }

    @Test
    public void stressWithOneThousandCachePages() throws Exception {
        DefaultPageCursorTracer monitor;
        monitor = stressDefNoPages(1000);
        printMonitorStats("stressWithOneThousandCachePages", monitor);
    }

    /** Run a stress test using a given number of pages
     *
     * @param pages
     * @return
     */
    private DefaultPageCursorTracer stressDefNoPages(int pages) throws Exception
    {
        DefaultPageCacheTracer monitor = new DefaultPageCacheTracer();
        DefaultPageCursorTracer cursorTracer = new DefaultPageCursorTracer();
        cursorTracer.init( monitor );
        Condition condition = numberOfEvictions(monitor, 100_000);

        PageCacheStressTest runner = new PageCacheStressTest.Builder()
                .withWorkingDirectory(testDirectory.directory())
                .withNumberOfCachePages(pages)
                .with(monitor)
                .with(condition)
                .withPageCursorTracerSupplier( () -> cursorTracer)
                .build();

        runner.run();
        return cursorTracer;
    }

    /** Prints the statistics stored in the PageCacheCounters object
     *
     * @param monitor
     */
    private void printMonitorStats(String testName, DefaultPageCursorTracer monitor)
    {
        System.out.println(String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s",testName, monitor.faults(),monitor.evictions(), monitor.pins(), monitor.hits(), monitor.hitRatio(), monitor.flushes(), monitor.bytesWritten(), monitor.bytesRead()));
    }

    void printEightyChars(String c)
    {
        for (int i = 0; i < 80; i++)
        {
            System.out.print(c);
        }
        System.out.println();
    }



}
