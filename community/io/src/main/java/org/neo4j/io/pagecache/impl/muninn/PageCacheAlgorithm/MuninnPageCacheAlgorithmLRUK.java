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
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MuninnPageCacheAlgorithmLRUK implements PageCacheAlgorithm
{

    private int cooperativeEvictionLiveLockThreshold;

    // Needs the instance of the page cache we're in
    // So we can access it's pages.
    private MuninnPageCache pageCache;

    private int kSize = 1;

    private long correlatedReferenceTimeout = 100000000;

    public static long referenceTime;

    //TODO Think of a better name for this variable.
    doubleLinkedPageMetaDataList dataPageList;

    private void resetReferenceTime()
    {
        referenceTime = System.nanoTime();
    }

    public MuninnPageCacheAlgorithmLRUK( int cooperativeEvictionLiveLockThreshold, MuninnPageCache pageCache, int kSize )
    {
        this.kSize = kSize;
        this.pageCache = pageCache;
        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        this.dataPageList = new doubleLinkedPageMetaDataList();
        resetReferenceTime();
    }


    public long cooperativlyEvict(PageFaultEvent faultEvent, PageList pages ) throws IOException {

        boolean evicted = false;
        int iterations = 0;
        doubleLinkedPageMetaDataList.Page evictionCandidatePage = null;

        long t = System.nanoTime();
        long minEvictionTime = t;


        synchronized ( this.dataPageList )
        {
            doubleLinkedPageMetaDataList.Page page = null;

            while (!evicted )
            {
                this.pageCache.assertHealthy();
                if ( this.pageCache.getFreelistHead() != null )
                {
                    return 0;
                }

                if (iterations >= 500)
                {
                    throw cooperativeEvictionLiveLock();
                }

                do
                {
                    if (page == null)
                    {
                        page = this.dataPageList.head;
                    }
                    else if ( page.next!= null )
                    {
                        page = page.next;
                    }
                    else
                    {
                        break;
                    }

                    System.out.println(t - page.pageData.getLastUsageTime());
                    System.out.println( page.pageData.getHistoryTime(this.kSize) < minEvictionTime);

                    if (t - page.pageData.getLastUsageTime() > this.correlatedReferenceTimeout &&
                            page.pageData.getHistoryTime(this.kSize) < minEvictionTime)
                    {
                        evictionCandidatePage = page;
                        minEvictionTime = page.pageData.getHistoryTime(kSize);
                    }


                } while (page.next != null);

                if (pages.isLoaded(evictionCandidatePage.pageRef) && pages.decrementUsage(evictionCandidatePage.pageRef))
                {
                    evicted = pages.tryEvict(evictionCandidatePage.pageRef, faultEvent);
                }

                iterations++;
            }

            if (evicted)
            {
                this.dataPageList.removePage(evictionCandidatePage.pageRef);
            }

            if (!evicted)
            {
                throw cooperativeEvictionLiveLock();
            }

            return evictionCandidatePage.pageRef;

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
                //Make sure we set the kSize!
                pageData.withKSize( this.kSize ).setAccessTime(0, pageData.getLastUsageTime());
                this.dataPageList.addPageFront( pageRef, pageData );
                return;
            }
            else if ( this.dataPageList.exists( pageRef ) )
            {
                doubleLinkedPageMetaDataList.Page page = this.dataPageList.findPage( pageRef );

                //If new, uncorrelated reference.
                if (System.nanoTime() - page.pageData.getLastUsageTime() >
                        this.correlatedReferenceTimeout)
                {

                   long correlPeriodOfRefdPage = page.pageData.getLastUsageTime() - page.pageData.getHistoryTime( 1);
                   System.out.println("Correl Period of refd page"+correlPeriodOfRefdPage);

                   for (int i = 2; i <= this.kSize; i++)
                   {
                       page.pageData.setAccessTime(i-1, page.pageData.getHistoryTime( i-1) + correlPeriodOfRefdPage);
                       System.out.println("***");
                   }

                   System.out.println("**");
                   //Add that last reference to the history times if we're not in correlated ref time.
                   page.pageData.setAccessTime ( 0, pageData.getLastUsageTime() );
                    System.out.println("***");

                }

                //Else, if correlated reference, or done doing uncorrelated ref stuff.
                this.dataPageList.setPageDataAndMoveToHead( pageRef, page.pageData );
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
