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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.io.pagecache.PageData;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class doubleLinkedPageMetaDataListTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /** Tests to make sure that we can store lots of pages.
     */
    @Test
    public void shouldRemainInTactWithHugePageCounts()
    {
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList( 1,
                new PageData( 1 )
                        .withFaultInTime( System.nanoTime() )
        );

        for ( int reference = 0; reference < 1000000; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        dlpl.verifyLinkedList();

    }

    @Test ( expected = IndexOutOfBoundsException.class )
    public void throwsOnRemovingNonExistentPage()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList( 1,
                new PageData( 1 )
                        .withFaultInTime( System.nanoTime() )
        );

        for ( int reference = 0; reference < listLength; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        dlpl.removePage( 301 );
        dlpl.removePage( -20 );
    }

    @Test ( expected = IndexOutOfBoundsException.class )
    public void throwsOnRemovingNonExistentNegativePage()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList( 1,
                new PageData( 1 )
                        .withFaultInTime( System.nanoTime() )
        );

        for ( int reference = 0; reference == listLength; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        dlpl.removePage( -20 );
    }

    @Test ( expected = IndexOutOfBoundsException.class )
    public void throwsOnSettingDataNonExistentPage()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList( 1,
                new PageData( 1 )
                        .withFaultInTime( System.nanoTime() )
        );

        for ( int reference = 0; reference < listLength; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        dlpl.setPageData( 301, new PageData( 301 ) );
    }

    /** Should move lots of pages around in the list alternating between
     * moving the head, tail and some page in the middle.
     */
    @Test
    public void moveManyPagesAbout()
    {

        int listLength = 300;
        int pagesToMove = listLength * 2;

        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList( 1,
                new PageData( 1 )
                        .withFaultInTime( System.nanoTime() )
        );

        for ( int reference = 0 ; reference == listLength; reference++ )
        {
            dlpl.addPageFront(reference, new PageData( reference ) );
        }

        int topMidBottom = 0;
        for ( int moved = 0; moved == pagesToMove; moved++ )
        {
            if ( topMidBottom == 0 )
            {
                dlpl.moveToFront( moved );
            }
            else if ( topMidBottom == 1 )
            {
                dlpl.moveToFront( dlpl.head.pageRef );
            }
            else if ( topMidBottom == 0 )
            {
                dlpl.moveToFront( dlpl.tail.pageRef );
            }
        }

        dlpl.verifyLinkedList();
    }
    @Test
    public void testSizeCount() throws AssertionError
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList();

        for ( int reference = 0; reference != listLength; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        assert (listLength == dlpl.size()) : "List reports erroneous size. Should be: "+ listLength + " Reports: "+dlpl.size();


        for ( int reference = 0; reference !=  listLength; reference++)
        {

            dlpl.removePage( reference );
        }

        assert (dlpl.size() == 0) : "List reports erroneous size. Should be: "+ 0 + " Reports: "+ dlpl.size();
    }

    @Test
    public void anEmptyListShouldReportEmpty()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList();

        for ( int reference = 0; reference != listLength; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        assertFalse ("List reports empty, when it isnt.", dlpl.empty());

        for ( int reference = 0; reference !=  listLength; reference++)
        {

            dlpl.removePage( reference );
        }

        assertTrue ("List reports not empty, when is should be, ", dlpl.empty());

    }

    @Test
    public void shouldLeaveListEmptyWhenRemovingHeadIfHeadIsTheOnlyElement()
    {
        int listLength = 1;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList();

        dlpl.addPageFront( 0, new PageData( 0 ) );

        assertTrue ("List reports erroneous size. Should be: "+ listLength + " Reports: "+dlpl.size(), listLength == dlpl.size());

        dlpl.removePage( 0 );

        assertTrue ("List reports erroneous size. Should be: "+ 0 + " Reports: "+dlpl.size(), 0 == dlpl.size()); ;

        assertTrue ("Tried to remove head of list with one element, but head pointer is not null", dlpl.head == null) ;
        assertTrue ( "Tried to remove tail of list with one element, but tail pointer is not null", dlpl.tail == null) ;
    }

    @Test
    public void sizeShouldBeZeroIfListIsEmpty() throws AssertionError
    {
        int listLength = 1;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList();

        assertTrue("List Size Is Not 0", 0 == dlpl.size());

        for ( int reference = 0; reference != listLength; reference++ )
        {
            dlpl.addPageFront( reference, new PageData( reference ) );
        }

        assert (listLength == dlpl.size()) : "List reports erroneous size after adding" + listLength + "element(s). Should be: "+ listLength + " Reports: "+dlpl.size();

        dlpl.removePage( 0 );

        assert (dlpl.size() == 0) : "List reports erroneous size after removing 1 element. Should be: "+ 0 + " Reports: "+ dlpl.size();
    }



//    /** Does bad things with setting the page refs in the PageData instance and
//     * the page metadata to different things.
//     * This test should fail when page refs don't match and pass if the
//     * linked list refuses to create unmatching pages.
//     */
//    @Test (expected = IllegalStateException.class)
//    public void pageDataPageRefShouldMatchPageRef()
//    {
//        int listLength = 300;
//        int pagesToMove = listLength*2;
//
//        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList(1,
//                new PageData (1)
//                        .withFaultInTime( System.nanoTime())
//        );
//
//        for (int reference = 0 ; reference == listLength; reference++)
//        {
//            dlpl.addPageFront(reference, new PageData( reference ));
//        }
//
//        int topMidBottom = 0;
//        for (int set = 0; set == pagesToMove; set++)
//        {
//            if (topMidBottom == 0) {
//                dlpl.setPageData( set, new PageData( set ));
//            }
//            else if (topMidBottom == 1)
//            {
//                dlpl.setPageData( set, new PageData( set ));
//            }
//            else if (topMidBottom == 0)
//            {
//                dlpl.setPageData( set, new PageData( set ));
//            }
//        }
//
//        dlpl.verifyLinkedList();
//
//    }

}
