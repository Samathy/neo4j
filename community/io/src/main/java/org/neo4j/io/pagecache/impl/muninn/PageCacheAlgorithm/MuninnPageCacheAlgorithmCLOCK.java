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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheAlgorithm;
import org.neo4j.io.pagecache.PageData;
import org.neo4j.io.pagecache.impl.muninn.CacheLiveLockException;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;

/** Implements the CLOCK Page Cache Eviction Algorithm
 *
 */
public final class MuninnPageCacheAlgorithmCLOCK implements PageCacheAlgorithm
{
    private int cooperativeEvictionLiveLockThreshold;

    // Needs the instance of the page cache we're in
    // So we can access it's pages.
    private MuninnPageCache pageCache;

    public MuninnPageCacheAlgorithmCLOCK( int cooperativeEvictionLiveLockThreshold, MuninnPageCache pageCache )
    {
        this.pageCache = pageCache;
        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
    }

    @Override
    public void setNumberOfPages( long maxPages )
    {

    }

    public long cooperativlyEvict( PageFaultEvent faultEvent, PageList pages ) throws IOException
    {
        /** Note this is called concurrently by Muninn, any object data stored should be
         thread safe.
         */

        int iterations = 0;
        int pageCount = pages.getPageCount( );
        int clockArm = ThreadLocalRandom.current( ).nextInt( pages.getPageCount( ) );
        long pageRef;
        boolean evicted = false;
        do
        {

            this.pageCache.assertHealthy( );
            if ( this.pageCache.getFreelistHead( ) != null )
            {
                return 0;
            }

            if ( clockArm == pageCount )
            {
                if ( iterations == this.cooperativeEvictionLiveLockThreshold )
                {
                    throw cooperativeEvictionLiveLock( );
                }
                iterations++;
                clockArm = 0;
            }

            pageRef = pages.deref( clockArm );
            if ( pages.isLoaded( pageRef ) && pages.decrementUsage( pageRef ) )
            {
                evicted = pages.tryEvict( pageRef, faultEvent );
            }
            clockArm++;
        }
        while ( !evicted );
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

    }

    @Override
    public void externalEviction( long pageRef, PageData pageData )
    {

    }

    @Override
    public synchronized void close( boolean debug )
    {

    }

    @Override
    public void printStatus()
    {

    }
}
