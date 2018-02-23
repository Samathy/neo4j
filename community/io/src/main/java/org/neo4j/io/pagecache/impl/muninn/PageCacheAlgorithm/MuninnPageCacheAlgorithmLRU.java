package org.neo4j.io.pagecache.impl.muninn.PageCacheAlgorithm;


import org.neo4j.io.pagecache.PageCacheAlgorithm;
import org.neo4j.io.pagecache.impl.muninn.CacheLiveLockException;
import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

/** Impliments the LRU Page Cache Eviction Algorithm
 *
 */
public class MuninnPageCacheAlgorithmLRU implements PageCacheAlgorithm
{
    private int cooperativeEvictionLiveLockThreshold;

    /** The local reference time.
     * This time is used to compare page entry times
     * too when they're getting evicted.
     *
     * As soon as the page-recency value >= Short.MAX_VALUE
     * we reset them all to a comparative recency (to each other)
     * And reset this value to the current time, too.
     */
    public static long referenceTime;
    //Static so we can see it elsewhere,
    // it being the same across all instances is a side effect that is
    // probably okay.

    private void resetReferenceTime()
    {
        referenceTime = Instant.now().getEpochSecond();
    }

    public MuninnPageCacheAlgorithmLRU( int cooperativeEvictionLiveLockThreshold )
    {
        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        resetReferenceTime();
    }

    /** \brief Finds the page with the next lowest recency
     *
     * @param pages
     * @param recency
     * @return
     */
    private long findPageWithRecencyLowerThan( PageList pages, short recency )
    {
        long pageRef = 0;
        long pageRefWithHighestRececncy = 0;
        short highestRecency = 0;

        for (int i = 0; i < pages.getPageCount(); i++)
        {
            short pageRecency = pages.getRecencyCounter( pageRef );

            if (pageRecency <= recency && pageRecency > highestRecency)
            {
                highestRecency = recency;
                pageRefWithHighestRececncy = pageRef;
            }
        }

        return pageRefWithHighestRececncy;
    }

    /** Reset the reference counts but so they're still in rough order.
     *
     * @param pages
     */
    private void resetReferenceCounts( PageList pages)
    {
        long pageRef;
        short highestRecency = Short.MAX_VALUE;
        int pageCount = pages.getPageCount();

        for (int i = 0; i < pages.getPageCount(); i++)
        {
            pageRef = findPageWithRecencyLowerThan( pages, highestRecency);
            highestRecency =  pages.getRecencyCounter( pageRef );
            if (pageCount <= Short.MAX_VALUE)
            {
                pages.setRecencyCounter(pageRef, (short)pageCount);
            }
            else
            {
                pages.setRecencyCounter(pageRef, Short.MAX_VALUE);
            }
        }
    }

    public long cooperativlyEvict(PageFaultEvent faultEvent, PageList pages) throws IOException {
        /** Note this is called concurrently by Muninn, any object data stored should be
         thread safe.
         */
        /* TODO we should check if the rececncy counts are filling up SHORT type and reset them */
        int pageCount = pages.getPageCount();
        long pageRef = 0;
        boolean evicted = false;

        int iterations = 0;

        //Candidates we tried to evict, but couldnt because they were locked.
        HashSet impossibleCandidates = new HashSet();

        long evictionCandidate = 0;
        short lowValue = 100;//Arbratary value. There'll almost certainly be a page with a smaller recency.

        do
        {
            /* Yup, linear search the page list! */
            /* Find the page with the oldest reference (smallest) */
            for (int i = 0; i < pages.getPageCount(); i++) {
                pageRef = pages.deref(i);


                if (!impossibleCandidates.contains(pageRef)) //Skip if we already know we can't evict it
                {
                    //Set the low value to the first non-impossible value we check.
                    if (i ==0)
                    {
                        lowValue = pages.getRecencyCounter( pageRef );
                    }
                    short recency = pages.getRecencyCounter(pageRef);
                    if (recency <= lowValue)
                    {
                        evictionCandidate = pageRef;
                        lowValue = recency;
                    }
                }
            }

            //Try to evict
            if (pages.isLoaded(evictionCandidate) && pages.decrementUsage(evictionCandidate) && !impossibleCandidates.contains( evictionCandidate )) {
                evicted = pages.tryEvict(evictionCandidate, faultEvent);
            }

            if (!evicted) // We failed to evict a page. Mark that one as checked and try again.
            {
                //We've tried to evict every single page and can't evict any of them.
                //We could check to see if cooperativeLiveLockThreshold < pageCount. In which case we should use that
                //Instead of the page count to trigger the exception.
                if ( impossibleCandidates.size() >= pageCount  || iterations >= pageCount)
                {
                    throw cooperativeEvictionLiveLock();
                }

                impossibleCandidates.add(evictionCandidate);
                lowValue = 100;
            }
            iterations++;
        }
        while ( !evicted );
        return evictionCandidate;


    }

    private CacheLiveLockException cooperativeEvictionLiveLock()
    {
        return new CacheLiveLockException(
                "Live-lock encountered when trying to cooperatively evict a page during page fault. " +
                        "This happens when we want to access a page that is not in memory, so it has to be faulted in, but " +
                        "there are no free memory pages available to accept the page fault, so we have to evict an existing " +
                        "page, but all the in-memory pages are currently locked by other accesses. If those other access are " +
                        "waiting for our page fault to make progress, then we have a live-lock, and the only way we can get " +
                        "out of it is by throwing this exception. This should be extremely rare, but can happen if the page " +
                        "cache size is tiny and the number of concurrently running transactions is very high. You should be " +
                        "able to get around this problem by increasing the amount of memory allocated to the page cache " +
                        "with the `dbms.memory.pagecache.size` setting. Please contact Neo4j support if you need help tuning " +
                        "your database." );
    }



}
