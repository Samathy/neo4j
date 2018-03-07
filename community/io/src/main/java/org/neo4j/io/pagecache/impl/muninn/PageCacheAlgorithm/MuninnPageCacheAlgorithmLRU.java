/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.muninn.PageCacheAlgorithm;

import org.neo4j.io.pagecache.PageCacheAlgorithm;
import org.neo4j.io.pagecache.PageData;
import org.neo4j.io.pagecache.impl.muninn.CacheLiveLockException;
import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;

import java.io.IOException;

import static java.lang.String.format;

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

    /** Mirrors the actual page list, but just stores metadata about pages and is sorted */
    doubleLinkedPageMetaDataList dataPageList =  new doubleLinkedPageMetaDataList( );

    private void resetReferenceTime()
    {
        referenceTime = System.nanoTime();
    }

    public MuninnPageCacheAlgorithmLRU( int cooperativeEvictionLiveLockThreshold )
    {
        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        resetReferenceTime();
    }

    public long cooperativlyEvict( PageFaultEvent faultEvent, PageList pages ) throws IOException
    {

        boolean evicted = false;
        int iterations = 0;
        int pageCount = pages.getPageCount();
        doubleLinkedPageMetaDataList.Page evictionCandidatePage;
        long evictionCandidate;

        synchronized ( this.dataPageList )
        {
            pageCount = pages.getPageCount();
            evictionCandidatePage = this.dataPageList.tail;
            evictionCandidate = evictionCandidatePage.pageRef;

            try
            {
                if ( pages.isLoaded( evictionCandidate ) && pages.decrementUsage( evictionCandidate ) )
                {
                    evicted = pages.tryEvict(evictionCandidate, faultEvent);
                }

                while ( !evicted )
                {
                    if ( iterations >= this.cooperativeEvictionLiveLockThreshold )
                    {
                        throw cooperativeEvictionLiveLock();
                    }
                    if ( evictionCandidatePage.last != null )
                    {
                        evictionCandidatePage = evictionCandidatePage.last;
                        evictionCandidate = evictionCandidatePage.pageRef;
                    }

                    if ( pages.isLoaded( evictionCandidate ) && pages.decrementUsage( evictionCandidate ) )
                    {
                        evicted = pages.tryEvict(evictionCandidate, faultEvent);
                    }

                    iterations++;
                }

            }
            catch ( Exception e )
            {
                throw e;
            }

            if ( evicted )
            {
                this.dataPageList.removePage( evictionCandidatePage.pageRef );
            }

            if ( !evicted )
            {
                throw cooperativeEvictionLiveLock();
            }

            return evictionCandidate;
        }
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

    @Override
    public void notifyPin( long pageRef, PageData pageData )
    {
        synchronized ( this.dataPageList )
        {
            if ( !this.dataPageList.exists( pageRef ) )
            {
                this.dataPageList.addPageFront( pageRef, pageData );
                return;
            }
            else if ( this.dataPageList.exists( pageRef ) )
            {
                this.dataPageList.setPageDataAndMoveToHead( pageRef, pageData );
                return;
            }
        }
    }

    @Override
    public void externalEviction( long pageRef, PageData pageData )
    {
        synchronized ( this.dataPageList )
        {
            if ( this.dataPageList.exists( pageRef ) )
            {
                this.dataPageList.removePage( pageRef );
            }
        }
        return;
    }
}
