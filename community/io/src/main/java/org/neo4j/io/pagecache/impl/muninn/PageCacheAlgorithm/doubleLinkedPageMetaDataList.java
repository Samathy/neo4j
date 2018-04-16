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

import org.neo4j.io.pagecache.PageData;

import static java.lang.String.format;

/** Store the page references in a doubly linked , ordered list.
 * When a page is referenced, move it to the head of the list, making the tail of the list store
 * what is probably the oldest
 * the oldest page.
 */
public class doubleLinkedPageMetaDataList
{
        /** Page. Store many of these */
        class Page
        {
            Page next;
            Page last;

            PageData pageData;
            long pageRef;

            Page( long pageRef, PageData pageData )
            {

                this.last = null;
                this.next = null;
                this.pageData = pageData;
                this.pageRef = pageRef;
            }

        }

        Page head;
        Page tail;

        //The number of pages currently logged in this list.
        private long size;

        /* Blank constructor needed if we want to have a page list but don't start it with any pages. */
        public doubleLinkedPageMetaDataList( )
        {

        }

        public doubleLinkedPageMetaDataList( long pageRef, PageData pageData )
        {
            this.head = new Page( pageRef, pageData );
            this.tail = this.head;

            this.size++;

        }

        public synchronized long size( )
        {
            if ( this.size < 0 )
            {
                System.out.println( "What the heck?! " + this.size );
            }
            return this.size;
        }

        /** Find a given page in the list
         *
         * @param pageRef
         * @return
         * @throws IndexOutOfBoundsException
         */
        public synchronized Page findPage( long pageRef ) throws IndexOutOfBoundsException
        {
            Page page;

            int iterations = 0;

            try
            {
                if ( this.head != null )
                {
                    page = this.head;
                }
                else
                {
                    throw noSuchPage( pageRef );
                }

                while ( page.next != null )
                {
                    if ( page.pageRef == pageRef )
                    {
                        break;
                    }

                    if ( page.next != null )
                    {
                        page = page.next;
                    }

                    iterations++;

                }

                if ( page.pageRef != pageRef )
                {
                    throw noSuchPage( pageRef );
                }
            }

            catch ( Exception e )
            {
                throw e;
            }

            return page;

        }

        synchronized boolean exists( long pageRef )
        {
            Page page = null;
            try
            {
                page = findPage( pageRef );
            }
            catch ( Exception e )
            {
                return false;
            }
            return true;

        }

        synchronized boolean empty( )
        {

            if ( this.head == null && this.tail == null && this.size == 0 )
            {
                return true;
            }

            return false;

        }

        /** Add a new page to the list if it doesnt already exist
         *
         * @param pageRef
         * @param pageData
         */
        public synchronized Page addPageFront( long pageRef, PageData pageData )
        {
            Page newPage = new Page( pageRef, pageData );

            if ( this.head != null )
            {
                newPage.next = this.head;
                this.head.last = newPage;

                this.head = newPage;
            }

            if ( this.head == null )
            {
                this.head = newPage;
                this.tail = newPage;
            }

            this.size++;

            return newPage;
        }

        /** Move a given page to the front of the list
         *
         * @param pageRef
         */
        public synchronized Page moveToFront( long pageRef ) throws IndexOutOfBoundsException
        {
            Page page = null;

            try
            {
                page = findPage( pageRef );
            }
            catch ( Exception e )
            {
                throw e;
            }

            //page should be this.head
            if ( page.last == null )
            {
                if ( page != this.head )
                {
                    System.out.println( "Oppsie! Is not head!" );
                }

                return page;
            }

            //Page is in the middle
            else if ( page.last != null && page.next != null )
            {
                page.last.next = page.next;
                page.next.last = page.last;

                page.last = null;

                page.next = this.head;
                this.head.last = page;
                this.head = page;

                return page;
            }

            //Page is at the end
            else if ( page.next == null && page.last != null )
            {
                page.last.next = null;
                this.tail = page.last;

                page.next = this.head;
                this.head.last = page;
                this.head = page;
                page.last = null;

                return page;
            }

            return page;
        }

        public synchronized void removePage( long pageRef ) throws IndexOutOfBoundsException
        {

            Page page = null;
            try
            {
                page = findPage( pageRef );
            }
            catch ( Exception e )
            {
                throw e;
            }

            //If we have both a next and a last, remove ourselves
            if ( page.last != null && page.next != null )
            {
                page.last.next = page.next;
                page.next.last = page.last;
            }

            //We're the tail, probably.
            else if ( page.last != null  && page.next == null )
            {
                page.last.next = null;
                this.tail = page.last;
            }

            //We're the head, probably.
            else if ( page.last == null && page.next != null )
            {
                page.next.last = null;
                this.head = page.next;
            }

            //We're the top, and only, element in the list
            else if ( page.last == null && page.next == null )
            {
                this.head = null;
                this.tail = null;
            }

            if ( this.size - 1 < 0 )
            {
                System.out.println( " Would make it negative" );
            }
            this.size--;

        }

        public synchronized void setPageData( long pageRef, PageData pageData )
        {
            Page page = null;
            try
            {
                page = findPage( pageRef );
            }
            catch ( Exception e )
            {
                throw e;
            }

            page.pageData = pageData;
        }

        public synchronized void setPageDataAndMoveToHead( long pageRef, PageData pageData )
        {
            try
            {
                setPageData( pageRef, pageData );
                moveToFront( pageRef );

            }
            catch ( Exception e )
            {
                throw e;
            }

        }

        public synchronized boolean verifyLinkedList( ) throws IllegalStateException
        {
            Page page = null;

            if ( this.head != null )
            {
                page = this.head;
            }
            else
            {
                throw linkedListErrorState( "HEAD is null" );
            }

            if ( page.last != null )
            {
                throw linkedListErrorState( "HEAD.last != null" );
            }

            if ( page.next == null )
            {
                if ( page != this.tail )
                {
                    throw linkedListErrorState( "HEAD.next == null" );
                }
            }

            while ( page.next != null )
            {
                Page lastPage = page;
                page = page.next;

                if ( page.last != lastPage )
                {
                    throw linkedListErrorState( format( "Last page marker is in correct for page %d", page.pageRef ) );
                }

                if ( page.next != null )
                {
                    continue;
                }
                else if ( page.next == null && page == this.tail )
                {
                    break;
                }
                else if ( page.next == null && page.next != this.tail )
                {
                    throw linkedListErrorState( "page.next == null BUT page != this.tail" );
                }
                else if ( page.next != null && page == this.tail )
                {
                    throw linkedListErrorState( "Page.next != null BUT page == this.tail" );
                }

            }

            if ( page == this.tail )
            {
                if ( page.next != null )
                {
                    throw linkedListErrorState( "Page.next is != null BUT page == this.tail" );
                }
            }

            return true;
        }

        private IndexOutOfBoundsException noSuchPage( long pageRef )
        {
            String msg = format( "Could not find page with page ID of %d", pageRef );
            return new IndexOutOfBoundsException( msg );
        }

        private IllegalStateException linkedListErrorState( String errorMessage )
        {
            String msg = format( " Cache algorithm Linked List is in an error state: %s", errorMessage );
            return new IllegalStateException( msg );
        }
}
