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
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class MuninnPageCacheAlgorithmLRUK implements PageCacheAlgorithm
{

    private int cooperativeEvictionLiveLockThreshold;

    // Needs the instance of the page cache we're in
    // So we can access it's pages.
    private MuninnPageCache pageCache;

    private int kSize = 2;

    private long correlatedReferenceTimeout = 3;

    private int referencesT;

    public static long referenceTime;

    //TODO Think of a better name for this variable.
    doubleLinkedPageMetaDataList dataPageList;

    private void resetReferenceTime( )
    {
        referenceTime = System.nanoTime( );
    }

    @Override
    public void setNumberOfPages( long maxPages )
    {

    }

    public MuninnPageCacheAlgorithmLRUK( int cooperativeEvictionLiveLockThreshold, MuninnPageCache pageCache, int kSize )
    {
        this.kSize = kSize;
        this.pageCache = pageCache;
        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        this.dataPageList = new doubleLinkedPageMetaDataList( );
        resetReferenceTime( );
    }

    public long cooperativlyEvict( PageFaultEvent faultEvent, PageList pages ) throws IOException
    {

        boolean evicted = false;
        int iterations = 0;
        doubleLinkedPageMetaDataList.Page evictionCandidatePage = null;

        long t = 0;
        long minEvictionTime = 0;

        Vector tried = new Vector( );
        doubleLinkedPageMetaDataList.Page page = null;

            t = this.referencesT;
            minEvictionTime = t;

            while ( !evicted )
            {
                this.pageCache.assertHealthy( );
                if ( this.pageCache.getFreelistHead( ) != null )
                {
                    return 0;
                }

                if ( iterations >= this.cooperativeEvictionLiveLockThreshold )
                {
                    throw cooperativeEvictionLiveLock( );
                }

                do
                {
                    this.pageCache.assertHealthy( );
                    if ( this.pageCache.getFreelistHead( ) != null )
                    {
                        return 0;
                    }

                    if ( page == null || page.last == null )
                    {
                        page = this.dataPageList.tail;
                    }
                    else if ( page.last != null )
                    {
                        page = page.last;
                    }
                    else if ( page.last == null )
                    {
                        break;
                    }

                    if ( t - page.pageData.getLastUsageTime( ) > this.correlatedReferenceTimeout &&
                            page.pageData.getHistoryTime( this.kSize ) <= minEvictionTime &&
                            !tried.contains( page.pageRef ) )
                    {
                        evictionCandidatePage = page;
                        minEvictionTime = page.pageData.getHistoryTime( this.kSize );
                    }

                }
                while ( page.last != null );

                if ( pages.isLoaded( evictionCandidatePage.pageRef ) && pages.decrementUsage( evictionCandidatePage.pageRef ) )
                {
                    evicted = pages.tryEvict( evictionCandidatePage.pageRef, faultEvent );
                }
                if ( !evicted && !tried.contains( evictionCandidatePage.pageRef ) )
                {
                    tried.add( evictionCandidatePage.pageRef );
                }

                iterations++;
            }

        if ( evicted )
        {
            this.dataPageList.removePage( evictionCandidatePage.pageRef );
        }

        if ( !evicted )
        {
            throw cooperativeEvictionLiveLock( );
        }

        return evictionCandidatePage.pageRef;

    }

    private CacheLiveLockException cooperativeEvictionLiveLock( )
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
            synchronized ( this )
            {
                this.referencesT++;
            }

            int t = this.referencesT;

            synchronized ( this.dataPageList )
            {
                if ( !this.dataPageList.exists( pageRef ) )
                {
                    PageData newPageData = new PageData( pageRef, this.kSize );

                    //In the actual LRU-k pseudocode they use a for loop to
                    // zero out the history time. We omit that here.

                    //Set last usage time and first history element to fault in time.
                    newPageData.setAccessTime( 1, t );
                    newPageData.setLastUsageTime( t );

                }
                else if ( this.dataPageList.exists( pageRef ) )
                {
                    return;
                }
                else if ( this.dataPageList.exists( pageRef ) )
                {
                    doubleLinkedPageMetaDataList.Page page = this.dataPageList.findPage( pageRef );

                    //If new, uncorrelated reference.
                    if ( ( t - page.pageData.getLastUsageTime( ) ) >
                            this.correlatedReferenceTimeout )
                    {

                        long correlPeriodOfRefdPage = page.pageData.getLastUsageTime( ) - page.pageData.getHistoryTime( 1 );

                        for ( int i = 2; i <= this.kSize; i++ )
                        {
                            page.pageData.setAccessTime( i, page.pageData.getHistoryTime( i - 1 ) + correlPeriodOfRefdPage );
                        }
                        //Set the last access time in history and last access time.
                        page.pageData.setAccessTime( 1, t );
                        page.pageData.setLastUsageTime( t );
                        dataPageList.moveToFront( pageRef );
                    }
                     else
                    {
                        //If correlated reference, just set the last access time
                        page.pageData.setLastUsageTime( t );
                    }

                    return;
                }
            }
        }

    @Override
    public void externalEviction( long pageRef, PageData pageData )
    {
        if ( this.dataPageList.exists( pageRef ) )
        {
            this.dataPageList.removePage( pageRef );
        }
        return;
    }

    @Override
    public synchronized void close( boolean debug )
    {
        if ( debug )
        {
            printStatus();
        }
    }

    @Override
    public void printStatus()
    {
        long iterations;

        doubleLinkedPageMetaDataList.Page page = this.dataPageList.head;

        System.out.println("A1List: ");

        for ( iterations = 0; iterations < this.dataPageList.size(); iterations++ )
        {
            String msg = "[PageRef: " + page.pageRef + "LastUsageTime: " + page.pageData.getLastUsageTime() +
                    " FaultInTime: " + page.pageData.getFaultInTime() + " References: " + page.pageData.getRefCount() +
                    " Old: " + page.pageData.isOld() + " New: " + page.pageData.isNew() + " ]";

            if ( this.dataPageList.head == page )
            {
                msg = msg + "<--HEAD";
            }
            else if ( this.dataPageList.tail == page )
            {
                msg = msg + "<--TAIL";
            }

            System.out.println(msg);

            page = page.next;
        }

        System.out.println("dataPageList size: " + this.dataPageList.size());

        System.out.println("CorrelatedReferenceTimeout: " + this.correlatedReferenceTimeout);
        System.out.println("kSize" + this.kSize);
    }

}
