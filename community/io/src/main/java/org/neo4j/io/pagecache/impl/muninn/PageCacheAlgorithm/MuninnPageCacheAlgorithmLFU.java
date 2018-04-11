package org.neo4j.io.pagecache.impl.muninn.PageCacheAlgorithm;

import org.neo4j.io.pagecache.PageCacheAlgorithm;
import org.neo4j.io.pagecache.PageData;
import org.neo4j.io.pagecache.impl.muninn.CacheLiveLockException;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;

import java.io.IOException;
import java.util.HashMap;

import static java.lang.Math.round;
import static java.lang.String.format;

public class MuninnPageCacheAlgorithmLFU implements PageCacheAlgorithm
{

    int cooperativeEvictionLiveLockThreshold;

    MuninnPageCache pageCache;

    long cacheSize;

    doubleLinkedPageMetaDataList pageList = new doubleLinkedPageMetaDataList();
    doubleLinkedPageMetaDataList.Page new_boundary;
    doubleLinkedPageMetaDataList.Page old_boundary;

    HashMap<Long, doubleLinkedPageMetaDataList> countChainList = new HashMap();

    long Cmax = 0;

    /** The proportions of the sections.
     *  Suggested: fOld <= 1- fNew
     *
     *  fOld = 1- fNew results in a middle size of 0
     *  fOld = 1/(cacheSize) = LRU policy.
     */
    long fOld;
    long fNew;


    public MuninnPageCacheAlgorithmLFU ( int cooperativeEvictionLiveLockThreshold, MuninnPageCache pageCache )
    {

        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        this.pageCache = pageCache;

        this.new_boundary = this.pageList.head;
        this.old_boundary = this.pageList.head;

        this.countChainList.put(new Long(0),new doubleLinkedPageMetaDataList() );
    }

    public void setNumberOfPages( long maxPages )
    {
        this.cacheSize = maxPages;
        ///this.fNew = round(this.cacheSize / 3);
        ///this.fOld =  cacheSize/2 - fNew;
        this.fNew = 333;
        this.fOld = 900;
    }

//    private long updateCmax()
//    {
//
//        this.Cmax = this.countChainList.keySet (this.countChainList.keySet().size());
//
//    }

    @Override
    public long cooperativlyEvict( PageFaultEvent faultEvent, PageList pages ) throws IOException
    {
        boolean evicted = false;
        int iterations = 0;
        int pageCount = pages.getPageCount();
        doubleLinkedPageMetaDataList.Page evictionCandidatePage = null;
        long evictionCandidate = 0;
        long count = 0;


        synchronized ( this.pageList )
        {
            synchronized (this.countChainList)
            {
                while (!evicted)
                {
                    if ( count > this.Cmax )
                    {
                        break;
                    }
                    this.pageCache.assertHealthy();
                    if (this.pageCache.getFreelistHead() != null)
                    {
                        return 0;
                    }

                    if ( this.countChainList.containsKey(count) )
                    {
                        doubleLinkedPageMetaDataList chain = this.countChainList.get(count);
                        //Technically shouldnt exist if its 0, but worth checking
                        if (chain.size() > 0 )
                        {
                            if ( chain.tail.pageData.isOld() )
                            {
                                evictionCandidatePage = chain.tail;
                                evictionCandidate = chain.tail.pageRef;
                            }
                            else
                            {
                                count++;
                                continue;
                            }
                        }
                        else
                        {
                            count++;
                            continue;
                        }

                    }
                    else
                    {
                        count++;
                        continue;
                    }

                        if ( pages.isLoaded(evictionCandidate) && pages.decrementUsage(evictionCandidate ))
                        {
                            evicted = pages.tryEvict( evictionCandidate, faultEvent );
                            break;
                        }

                    count++;

                }

                if ( !evicted )
                {
                    System.out.println("Making LRU eviction attempt");
                    pageCount = pages.getPageCount();
                    evictionCandidatePage = this.pageList.tail;
                    evictionCandidate = evictionCandidatePage.pageRef;

                    this.pageCache.assertHealthy();
                    if ( this.pageCache.getFreelistHead() != null )
                    {
                        return 0;
                    }

                    if ( pages.isLoaded(evictionCandidate) && pages.decrementUsage(evictionCandidate ))
                    {
                        evicted = pages.tryEvict(evictionCandidate, faultEvent);
                    }

                    while ( !evicted )
                    {
                        this.pageCache.assertHealthy();
                        if ( this.pageCache.getFreelistHead() != null )
                        {
                            return 0;
                        }

                        if (iterations >= this.cooperativeEvictionLiveLockThreshold)
                        {
                            throw cooperativeEvictionLiveLock();
                        }
                        if (evictionCandidatePage.last != null)
                        {
                            evictionCandidatePage = evictionCandidatePage.last;
                            evictionCandidate = evictionCandidatePage.pageRef;
                        }
                        //TODO We should probs do something if we've got to the HEAD of the list

                        if ( pages.isLoaded(evictionCandidate) && pages.decrementUsage(evictionCandidate ))
                        {
                            evicted = pages.tryEvict(evictionCandidate, faultEvent);
                        }

                        iterations++;
                    }
                }

                if ( evicted )
                {
                    long chainElement = evictionCandidatePage.pageData.getRefCount();

                    this.countChainList.get(chainElement).removePage(evictionCandidate);

                    if ( this.countChainList.get(chainElement).size() == 0 && chainElement != 0 )
                    {
                        this.countChainList.remove(chainElement);
                    }
                    this.pageList.removePage(evictionCandidate);
                }

                if (!evicted)
                {
                    throw cooperativeEvictionLiveLock();
                }

                return evictionCandidate;

            }
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

        //All pages added should be marked as until the new boundary is at 1/3 of the cache size.
        //Then pages can be marked as not-new and not-old until the old boundary is 2/3 of the cache size.

        synchronized ( this.pageList )
        {
            synchronized ( this.countChainList )
            {
                if ( !this.pageList.exists(pageRef) )
                {
                    //It'd be nice if we could use the same page object for both lists, to save memory.
                    this.pageList.addPageFront(pageRef, pageData);
                    try
                    {
                        this.countChainList.get(new Long(0)).addPageFront(pageRef, pageData); //Should never be 0, so we're probably fine to not error check.
                    } catch ( Exception e )
                    {
                        throw e;
                    }

                    if (this.pageList.size() == 1)
                    {
                        this.new_boundary = this.pageList.head;
                        this.old_boundary = this.pageList.head;
                    } else
                    {

                        if (this.new_boundary.last != null)
                        {
                            this.new_boundary = this.new_boundary.last;
                        }
                        this.new_boundary.pageData.setNew(false);

                        if (this.old_boundary.last != null)
                        {
                            this.old_boundary = this.old_boundary.last;
                        }
                        this.old_boundary.pageData.setOld(true);
                    }

                    return;
                } else if ( this.pageList.exists(pageRef) )
                {
                    doubleLinkedPageMetaDataList.Page page = this.pageList.findPage(pageRef);

                    //Keep moving the boundary pointers up the list until they're in the right places.
                    // Or, if they're already in the right place just action on the pages.
                    if ( this.pageList.size() < this.fNew || this.pageList.size() < this.fOld)
                    {
                        if ( this.pageList.size() < this.fNew )
                        {
                            this.new_boundary = this.pageList.tail;
                        }

                        if ( this.pageList.size() < this.fOld )
                        {
                            this.old_boundary = this.pageList.tail;
                        }
                    }
                    else if ( page.pageData.isOld() )
                    {
                        if ( this.new_boundary.last != null )
                        {
                            this.new_boundary = this.new_boundary.last;
                        }
                        this.new_boundary.pageData.setNew(false);

                        if ( this.old_boundary.last != null )
                        {
                            this.old_boundary = this.old_boundary.last;
                        }
                        this.old_boundary.pageData.setOld(true);
                    }

                    pageData.setNew(true);
                    pageData.setOld(false);

                    doubleLinkedPageMetaDataList list = null;
                    //Swap it into its new count chain list location
                    try
                    {
                         list = this.countChainList.get(page.pageData.getRefCount());
                        list.removePage(pageRef);
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }

                    //Clean up if we just removed the last element
                    if ( this.countChainList.get (  page.pageData.getRefCount() ).size() == 0)
                    {
                        if ( page.pageData.getRefCount() != 0)
                        {
                            this.countChainList.remove(page.pageData.getRefCount());
                        }
                    }

                    pageData.setRefCount( page.pageData.getRefCount()+1); //TODO correlated references
                    this.pageList.setPageDataAndMoveToHead(pageRef, pageData);

                    //If the countList already contains a double linked list for this key
                    //then just add it to the top of that.
                    //Else, make a new list.
                    if ( this.countChainList.containsKey( page.pageData.getRefCount() ))
                    {
                        this.countChainList.get( page.pageData.getRefCount() ).addPageFront( page.pageRef, pageData );
                    }
                    else
                    {
                        this.countChainList.put( page.pageData.getRefCount(), new doubleLinkedPageMetaDataList());
                        this.countChainList.get( page.pageData.getRefCount() ).addPageFront( page.pageRef, pageData );

                        if (  page.pageData.getRefCount() > this.Cmax)
                        {
                            this.Cmax++;
                        }
                    }

                    return;
                }
            }
        }

    }

    @Override
    public void externalEviction( long pageRef, PageData pageData )
    {
        synchronized ( this.pageList )
        {
            synchronized ( this.countChainList )
            {
                if ( this.pageList.exists(pageRef) )
                {
                    try
                    {
                        doubleLinkedPageMetaDataList list = this.countChainList.get(this.pageList.findPage(pageRef).pageData.getRefCount());
                        list.removePage(pageRef);
                    }
                    catch ( Exception e)
                    {
                        throw e;
                    }
                    this.pageList.removePage(pageRef);
                    //Should probably re-align boundarys.
                }
            }

        }
        return;

    }



    private void printStatus ()
    {
        doubleLinkedPageMetaDataList.Page page = this.pageList.head;

        while ( page != null)
        {

            String msg = format( "PageRef: "+ page.pageRef+ " References: "+ page.pageData.getRefCount()+" isNew: "+page.pageData.isNew()+" isOld: "+page.pageData.isOld()+"");
            if (this.new_boundary == page)
            {
                msg = msg + "<-- new_boundary";
            }
            if (this.old_boundary == page)
            {
                msg = msg + "<-- old_boundary";
            }
            System.out.println(msg);

            page = page.next;

        }



    }
}
