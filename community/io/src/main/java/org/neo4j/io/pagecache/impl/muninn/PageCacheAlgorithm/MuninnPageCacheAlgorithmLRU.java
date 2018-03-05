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

    /** Store the page references in a doubly linked , ordered list.
     * When a page is referenced, move it to the head of the list, making the tail of the list store
     * what is probably the oldest
     * the oldest page.
     */
    class doubleLinkedPageList
    {

        /** Page. Store many of these */
        class Page
        {
            Page next;
            Page last;

            PageData pageData;
            long pageRef;

            public Page( long pageRef, PageData pageData)
            {

                this.last = null;
                this.next = null;
                this.pageData = pageData;
                this.pageRef = pageRef;
            }

        }

        Page head = null;
        Page tail = null;

        //Should we verify the list every time we do something with it?
        //Usful for tests/debugging
        boolean verifyList = false;

        Page doubleLinkedPageList (long pageRef, PageData pageData)
        {
            this.head = new Page ( pageRef, pageData);
            this.tail = this.head;

            return this.head;
        }

        Page doubleLinkedPageList (long pageRef, PageData pageData, boolean verifyList)
        {
            this.verifyList = verifyList;
            this.head = new Page ( pageRef, pageData);
            this.tail = this.head;

            return this.head;
        }

        /** Find a given page in the list
         *
         * @param pageRef
         * @return
         * @throws IndexOutOfBoundsException
         */
        private synchronized Page findPage (long pageRef ) throws IndexOutOfBoundsException {
            Page page;

            int iterations = 0;

            try {
                if (this.head != null) {
                    page = this.head;
                } else {
                    throw noSuchPage(pageRef);
                }


                while (page.next != null) {

                    if (page.pageRef == pageRef) {
                        break;
                    }

                    if (page.next != null) {
                        page = page.next;
                    }

                    iterations++;

                }

                if (page.pageRef != pageRef) {
                    throw noSuchPage(pageRef);
                }
            }

            catch (Exception e)
            {
                throw e;
            }

            return page;

        }

        synchronized boolean exists ( long pageRef )
        {
            Page page = null;
            try
            {
                page = findPage(pageRef);
            }
            catch  (Exception e)
            {
                return false;
            }
            return true;

        }

        /** Add a new page to the list if it doesnt already exist
         *
         * @param pageRef
         * @param pageData
         */
        public synchronized Page addPageFront( long pageRef, PageData pageData )
        {

            //It might be the case that the list has no pages in yet, and we're the first.
            if ( this.head != null ) {
                verifyLinkedList();
            }

            Page newPage = new Page( pageRef, pageData);

            if (this.head != null)
            {
                newPage.next = this.head;
                this.head.last = newPage;

                this.head = newPage;
            }

            if (this.head == null)
            {
                this.head = newPage;
                this.tail = newPage;
            }

            verifyLinkedList();

           return newPage;
        }

        /** Move a given page to the front of the list
         *
         * @param pageRef
         */
        public synchronized Page moveToFront( long pageRef ) throws IndexOutOfBoundsException
        {
            verifyLinkedList();

            Page page = null;

            try
            {
                page = findPage (pageRef);
            }
            catch (Exception e)
            {
               throw e;
            }


            //page should be this.head
            if (page.last == null) {
                if (page != this.head) {
                    System.out.println("Oppsie! Is not head!");
                }

                verifyLinkedList();

                return page;
            }

            //Page is in the middle
            else if (page.last != null && page.next != null) {
                page.last.next = page.next;
                page.next.last = page.last;

                page.last = null;

                page.next = this.head;
                this.head.last = page;
                this.head = page;

                verifyLinkedList();

                return page;
            }

            //Page is at the end
            else if ( page.next == null && page.last != null) {

                page.last.next = null;
                this.tail = page.last;

                page.next = this.head;
                this.head.last = page;
                this.head = page;
                page.last = null;

                verifyLinkedList();

                return page;
            }

            verifyLinkedList();

            return page;
        }

        public synchronized void removePage( long pageRef ) throws IndexOutOfBoundsException
        {
            verifyLinkedList();

            Page page = null;
            try
            {
                 page = findPage(pageRef);
            }
            catch (Exception e)
            {
                throw e;
            }

            //If we have both a next and a last, remove ourselves
            if (page.last != null && page.next != null)
            {
                page.last.next = page.next;
                page.next.last = page.last;
            }

            //We're the tail, probably.
            if ( page.last != null  && page.next == null)
            {
                page.last.next = null;
                this.tail = page.last;
            }

            //We're the head, probably.
            if (page.last == null && page.next != null)
            {
                page.next.last = null;
                this.head = page.next;
            }

            verifyLinkedList();

        }

        public synchronized void setPageData ( long pageRef, PageData pageData )
        {
            Page page = null;
            try {
                page = findPage(pageRef);
            } catch (Exception e) {
                throw e;
            }

            page.pageData = pageData;
        }

        public synchronized void setPageDataAndMoveToHead( long pageRef, PageData pageData)
        {
            verifyLinkedList();

            try
            {
                setPageData(pageRef, pageData);

                verifyLinkedList();

                moveToFront( pageRef );

                verifyLinkedList();
            }
            catch (Exception e)
            {
                throw e;
            }

            verifyLinkedList();
        }

        public synchronized boolean verifyLinkedList () throws IllegalStateException
        {
            if (this.verifyList == false)
            {
                return true;
            }

            Page page = null;

            if (this.head != null) {
                page =this.head;
            }
            else {
                throw linkedListErrorState("HEAD is null");
            }

            if (page.last != null)
            {
                throw linkedListErrorState("HEAD.last != null");
            }

            if (page.next == null)
            {
                if (page != this.tail) {
                    throw linkedListErrorState("HEAD.next == null");
                }
            }

            while ( page.next != null) {
                Page lastPage = page;
                page = page.next;

                if (page.last != lastPage) {
                    throw linkedListErrorState(format("Last page marker is in correct for page %d", page.pageRef));
                }

                if (page.next != null) {
                    continue;
                } else if (page.next == null && page == this.tail) {
                    break;
                } else if (page.next == null && page.next != this.tail) {
                    throw linkedListErrorState("page.next == null BUT page != this.tail");
                } else if (page.next != null && page == this.tail) {
                    throw linkedListErrorState("Page.next != null BUT page == this.tail");
                }

            }

            if (page == this.tail)
            {
                if (page.next != null)
                {
                    throw linkedListErrorState("Page.next is != null BUT page == this.tail");
                }
            }

            return true;
        }

        private IndexOutOfBoundsException noSuchPage(long pageRef )
        {
            String msg = format("Could not find page with page ID of %d", pageRef);
            return new IndexOutOfBoundsException(msg);
        }

        private IllegalStateException linkedListErrorState (String errorMessage)
        {
            String msg = format (" Cache algorithm Linked List is in an error state: %s", errorMessage);
            return new IllegalStateException( msg);
        }


    }

    /** Mirrors the actual page list, but just stores metadata about pages and is sorted */
    doubleLinkedPageList dataPageList =  new doubleLinkedPageList();

    private void resetReferenceTime()
    {
        referenceTime = System.nanoTime();
    }

    public MuninnPageCacheAlgorithmLRU( int cooperativeEvictionLiveLockThreshold )
    {
        this.cooperativeEvictionLiveLockThreshold = cooperativeEvictionLiveLockThreshold;
        resetReferenceTime();
    }

    public long cooperativlyEvict ( PageFaultEvent faultEvent, PageList pages) throws IOException
    {

        boolean evicted = false;
        int iterations = 0;
        long evictionCandidate = this.dataPageList.tail.pageRef;
        int pageCount = pages.getPageCount();
        doubleLinkedPageList.Page evictionCandidatePage = this.dataPageList.tail;

        synchronized (this.dataPageList) {
            evictionCandidate = this.dataPageList.tail.pageRef;
            pageCount = pages.getPageCount();
            evictionCandidatePage = this.dataPageList.tail;

            try {

                if (pages.isLoaded(evictionCandidate) && pages.decrementUsage(evictionCandidate)) {
                    evicted = pages.tryEvict(evictionCandidate, faultEvent);
                }

                while (!evicted ) {
                    if ( iterations >= this.cooperativeEvictionLiveLockThreshold)
                    {
                        throw cooperativeEvictionLiveLock();
                    }
                    if (evictionCandidatePage.last != null) {
                        evictionCandidatePage = evictionCandidatePage.last;
                        evictionCandidate = evictionCandidatePage.pageRef;
                    }

                    if (pages.isLoaded(evictionCandidate) && pages.decrementUsage(evictionCandidate)) {
                        evicted = pages.tryEvict(evictionCandidate, faultEvent);
                    }

                    iterations++;
                }

            } catch (Exception e) {
                throw e;
            }

            if (evicted) {
                this.dataPageList.removePage(evictionCandidatePage.pageRef);
            }

            if (!evicted) {
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
    public void notifyPin(long pageRef, PageData pageData)
    {
        synchronized ( this.dataPageList) {
            if (!this.dataPageList.exists(pageRef)) {
                this.dataPageList.addPageFront(pageRef, pageData);
                return;
            } else if (this.dataPageList.exists(pageRef)) {
                this.dataPageList.setPageDataAndMoveToHead(pageRef, pageData);
                return;
            }
        }

    }
}
