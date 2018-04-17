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

public class MuninnPageCacheAlgorithm2Q implements PageCacheAlgorithm
{
    int cooperativeEvictionLiveLockThreshold;

    MuninnPageCache pageCache;

    long a1Size;
    long a2Size;

    long a1ListEvictions;
    long a2ListEvictions;

    //Entry list. FiFo managed, stores pages referenced once
    doubleLinkedPageMetaDataList a1List = new doubleLinkedPageMetaDataList( );

    //AM list, stores pages referenced more than once.
    //LRU managed
    doubleLinkedPageMetaDataList a2List = new doubleLinkedPageMetaDataList( );

    public MuninnPageCacheAlgorithm2Q( int cooperativeEvictionLiveLockThreshold, MuninnPageCache pageCache )
    {

        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        this.pageCache = pageCache;
    }

    public void setNumberOfPages( long maxPages )
    {
        this.a1Size = maxPages / 2;
        this.a2Size = maxPages / 2;
    }

    /**
     * Evicts a page from the FiFo managed A1 list
     **/
    private long a1Evict( PageList pages, PageFaultEvent faultEvent ) throws IOException
    {
        doubleLinkedPageMetaDataList.Page page = null;
        int iterations = 0;
        boolean evicted = false;

        synchronized ( this.a1List )
        {

            if ( !this.a1List.empty( ) )
            {
                page = this.a1List.tail;
                do
                {
                    if ( iterations >= this.a1List.size( ) )
                    {
                        return 0;
                    }

                    if ( pages.isLoaded(page.pageRef ) && pages.decrementUsage( page.pageRef ) )
                    {
                        evicted = pages.tryEvict( page.pageRef, faultEvent );
                    }
                    if ( evicted )
                    {
                        this.a1ListEvictions++;
                        break;
                    }

                    if ( page.last == null )
                    {
                        page = this.a1List.tail;
                    }
                    else
                    {
                        page = page.last;
                    }

                    iterations++;
                }
                while ( !evicted );

                if ( evicted )
                {
                    this.a1List.removePage( page.pageRef );
                }
            }
            else
            {
                return 0;
            }
        }

        return page.pageRef;
    }

    /**
     * Enters a page to the A1 list.
     **/
    private void a1Enter( long pageRef, PageData pageData )
    {
        this.a1List.addPageFront( pageRef, pageData );
    }

    /**
     * Evicts a page from the LRU managed A1 list
     **/
    private long a2Evict( PageList pages, PageFaultEvent faultEvent ) throws IOException
    {
        doubleLinkedPageMetaDataList.Page page = null;
        int iterations = 0;
        boolean evicted = false;

        synchronized ( this.a2List )
        {
            if ( !this.a2List.empty( ) )
            {
                page = this.a2List.tail;
                do
                {

                    if ( iterations >= this.a2List.size( ) )
                    {
                        return 0;
                    }

                    if ( pages.isLoaded(page.pageRef ) && pages.decrementUsage( page.pageRef ) )
                    {
                        evicted = pages.tryEvict( page.pageRef, faultEvent );
                    }
                    if ( evicted )
                    {
                        this.a2ListEvictions++;
                        break;
                    }

                    if ( page.last == null )
                    {
                        page = a2List.tail;
                    }
                    else
                    {
                        page = page.last;
                    }

                    iterations++;
                }
                while ( !evicted );

                if ( evicted )
                {
                    this.a2List.removePage( page.pageRef );
                }
            }
            else
            {
                return 0;
            }
        }
        return page.pageRef;
    }

    /**
     * Enters a page to the A1 list.
     **/
    private void a2Enter( long pageRef, PageData pageData )
    {
        this.a2List.addPageFront( pageRef, pageData );
    }

    @Override
    public long cooperativlyEvict( PageFaultEvent faultEvent, PageList pages ) throws IOException
    {

        long pageRef = 0;
        int iterations = 0;

        do
        {
            this.pageCache.assertHealthy( );
            if ( this.pageCache.getFreelistHead( ) != null )
            {
                return 0;
            }

            if ( this.a1List.empty( ) && this.a2List.empty( ) )
            {
                return 0;
            }

            if ( iterations >= this.cooperativeEvictionLiveLockThreshold )
            {
                throw cooperativeEvictionLiveLock( );
            }
            if ( !this.a1List.empty( ) )
            {
                pageRef = this.a1Evict( pages, faultEvent );

            }
            if ( pageRef == 0 && !this.a2List.empty( ) )
            {
                pageRef = this.a2Evict( pages, faultEvent );
            }

            //freelist head is null
            if ( pageRef == -1 )
            {
                return 0;
            }

            iterations++;

    }
    while ( pageRef == 0 );

    return pageRef;
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
        synchronized ( this.a1List )
        {
            synchronized ( this.a2List )
            {
                if ( !this.a1List.exists( pageRef ) && !this.a2List.exists( pageRef ) )
                {
                    if ( this.a1List.size( ) + 1 > this.a1Size )
                    {
                    }
                    this.a1Enter( pageRef, pageData );
                }
                else if ( this.a1List.exists( pageRef ) )
                {
                    if ( this.a2List.size( ) + 1 > this.a2Size )
                    {

                    }

                    this.a1List.removePage( pageRef );
                    this.a2Enter( pageRef, pageData );

                }
                else if ( this.a2List.exists( pageRef ) )
                {
                    this.a2List.setPageDataAndMoveToHead( pageRef, pageData );
                }

                return;
            }
        }

    }

    @Override
    public void externalEviction( long pageRef, PageData pageData )
    {
        synchronized ( this.a1List )
        {
            if ( this.a1List.exists( pageRef ) )
            {
                this.a1List.removePage( pageRef );
            }
        }

        synchronized ( this.a2List )
        {
            if ( this.a2List.exists( pageRef ) )
            {
                this.a2List.removePage( pageRef );
            }
        }

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
    public void printStatus( )
    {

        long iterations;

        doubleLinkedPageMetaDataList.Page page = this.a1List.head;

        System.out.println("A1List: ");

        for ( iterations = 0; iterations < this.a1List.size(); iterations++ )
        {
            String msg = "[PageRef: " + page.pageRef + "LastUsageTime: " + page.pageData.getLastUsageTime() +
                    " FaultInTime: " + page.pageData.getFaultInTime() + " References: " + page.pageData.getRefCount() +
                    " Old: " + page.pageData.isOld() + " New: " + page.pageData.isNew() + " ]";

             if ( this.a1List.head == page )
            {
                msg = msg + "<--HEAD";
            }
            else if ( this.a2List.tail == page )
            {
                msg = msg + "<--TAIL";
            }

            System.out.println(msg);
        }

        System.out.println("A2List: ");

        page = this.a2List.head;

        for ( iterations = 0; iterations < this.a2List.size(); iterations++ )
        {
            String msg = "[PageRef: " + page.pageRef + "LastUsageTime: " + page.pageData.getLastUsageTime() +
                    " FaultInTime: " + page.pageData.getFaultInTime() + " References: " + page.pageData.getRefCount() +
                    " Old: " + page.pageData.isOld() + " New: " + page.pageData.isNew() + " ]";

            if ( this.a2List.head == page )
            {
                msg = msg + "<--HEAD";
            }
            else if ( this.a2List.tail == page )
            {
                msg = msg + "<--TAIL";
            }

            System.out.println(msg);

            page = page.next;
        }

        System.out.println("CacheSize: " + ( this.a1Size + this.a2Size ) );
        System.out.println("A1List size: " + this.a1List.size());
        System.out.println("A2List size: " + this.a2List.size());

        System.out.println("A1List Evictions: " + this.a1ListEvictions);
        System.out.println("A2List Evictions: " + this.a2ListEvictions);

    }
}
