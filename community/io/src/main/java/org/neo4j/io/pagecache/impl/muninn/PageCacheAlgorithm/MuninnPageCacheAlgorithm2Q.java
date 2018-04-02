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
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.impl.muninn.PageCacheAlgorithm.doubleLinkedPageMetaDataList;

import java.io.IOException;

public class MuninnPageCacheAlgorithm2Q implements PageCacheAlgorithm
{
    int cooperativeEvictionLiveLockThreshold;

    MuninnPageCache pageCache;

    long a1Size;
    long a2Size;

    //Entry list. FiFo managed, stores pages referenced once
    doubleLinkedPageMetaDataList a1List = new doubleLinkedPageMetaDataList();

    //AM list, stores pages referenced more than once.
    //LRU managed
    doubleLinkedPageMetaDataList a2List = new doubleLinkedPageMetaDataList();

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


    /** Evicts a page from the FiFo managed A1 list **/
    private long a1Evict ()
    {
        long pageRef ;
        synchronized ( this.a1List )
        {
          pageRef = this.a1List.tail.pageRef;
        }

        this.a1List.removePage( pageRef );

        return pageRef;
    }

    /** Enters a page to the A1 list. **/
    private void a1Enter( long pageRef, PageData pageData)
    {
        this.a1List.addPageFront( pageRef, pageData );
    }

    /** Evicts a page from the LRU managed A1 list **/
    private long a2Evict ()
    {
        long pageRef ;
        synchronized ( this.a2List )
        {
            pageRef = this.a2List.tail.pageRef;
        }

        this.a2List.removePage( pageRef );

        return pageRef;
    }

    /** Enters a page to the A1 list. **/
    private void a2Enter( long pageRef, PageData pageData)
    {
        this.a2List.addPageFront( pageRef, pageData );
    }

    @Override
    public long cooperativlyEvict(PageFaultEvent faultEvent, PageList pages) throws IOException
    {

        int iterations = 0;
        doubleLinkedPageMetaDataList.Page page = null;
        boolean evicted = false;

        if ( this.a1List.size() >= this.a2List.size())
        {
            synchronized ( this.a1List ) {
                page = this.a1List.tail;
                while (!evicted) {

                    if (iterations > a1Size) {
                        System.out.println("Excessive iteratons on a1 list");
                    }

                    this.pageCache.assertHealthy();
                    if (this.pageCache.getFreelistHead() != null) {
                        return 0;
                    }


                    if (pages.isLoaded(page.pageRef) && pages.decrementUsage(page.pageRef)) {
                        evicted = pages.tryEvict(page.pageRef, faultEvent);
                    }
                    if (evicted) {
                        break;
                    }

                    page = page.last;

                    iterations++;
                }

                this.a1List.removePage(page.pageRef);
            }
        }
        else
        {
            synchronized ( this.a2List )
            {

                page = this.a2List.tail;
                while (!evicted) {

                    if (iterations > this.a2Size) {
                        System.out.println("Excessive iteratons on a2 list");
                    }

                    this.pageCache.assertHealthy();
                    if (this.pageCache.getFreelistHead() != null) {
                        return 0;
                    }


                    if (pages.isLoaded(page.pageRef) && pages.decrementUsage(page.pageRef)) {
                        evicted = pages.tryEvict(page.pageRef, faultEvent);
                    }
                    if (evicted) {
                        break;
                    }


                    if (page.last == null) {
                        System.out.println("Reached list HEAD");
                        page = a2List.tail;
                    } else {
                        page = page.last;
                    }
                }

                this.a2List.removePage(page.pageRef);
                iterations++;
            }
        }



        return page.pageRef;
    }

    @Override
    public void notifyPin(long pageRef, PageData pageData)
    {
        synchronized ( this.a1List)
        {
            synchronized ( this.a2List )
            {
                if (!this.a1List.exists(pageRef) && !this.a2List.exists(pageRef))
                {
                    if (this.a1List.size() + 1 > this.a1Size)
                    {
                    }
                    this.a1Enter(pageRef, pageData);
                } else if (this.a1List.exists(pageRef))
                {
                    if (this.a2List.size() + 1 > this.a2Size)
                    {

                    }

                    this.a1List.removePage(pageRef);
                    this.a2Enter(pageRef, pageData);

                } else if (this.a2List.exists(pageRef))
                {
                    this.a2List.setPageDataAndMoveToHead(pageRef, pageData);
                }

                return;
            }}

    }

    @Override
    public void externalEviction(long pageRef, PageData pageData)
    {

        synchronized ( this.a1List )
        {
            if (this.a1List.exists(pageRef))
            {
                this.a1List.removePage(pageRef);
            }
        }
       synchronized ( this.a2List )
       {
           if (this.a2List.exists(pageRef))
           {
               this.a2List.removePage(pageRef);
           }
       }


    }
}
