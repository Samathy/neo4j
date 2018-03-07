package org.neo4j.io.pagecache.impl.muninn.PageCacheAlgorithm;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.io.pagecache.PageCacheAlgorithm;
import org.neo4j.io.pagecache.PageData;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class doubleLinkedPageMetaDataListTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /** Tests to make sure that we can store lots of pages.
     */
    @Test
    public void shouldRemainInTactWithHugePageCounts()
    {

        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList(1,
                new PageData (1)
                        .withFaultInTime( System.nanoTime())
        );

        for (int reference = 0; reference < 1000000; reference++)
        {
            dlpl.addPageFront(reference, new PageData( reference ));
        }

        dlpl.verifyLinkedList();

    }

    @Test (expected = IndexOutOfBoundsException.class )
    public void throwsOnRemovingNonExistentPage()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList(1,
                new PageData (1)
                        .withFaultInTime( System.nanoTime())
        );

        for (int reference = 0; reference == listLength; reference++)
        {
            dlpl.addPageFront(reference, new PageData( reference ));
        }


        dlpl.removePage( 301 );
        dlpl.removePage( -20 );
    }

    @Test (expected = IndexOutOfBoundsException.class )
    public void throwsOnRemovingNonExistentNegativePage()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList(1,
                new PageData (1)
                        .withFaultInTime( System.nanoTime())
        );

        for (int reference = 0; reference == listLength; reference++)
        {
            dlpl.addPageFront(reference, new PageData( reference ));
        }

        dlpl.removePage( -20 );
    }



    @Test (expected = IndexOutOfBoundsException.class )
    public void throwsOnSettingDataNonExistentPage()
    {
        int listLength = 300;
        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList(1,
                new PageData (1)
                        .withFaultInTime( System.nanoTime())
        );

        for (int reference = 0; reference == listLength; reference++)
        {
            dlpl.addPageFront(reference, new PageData( reference ));
        }


        dlpl.setPageData( 301, new PageData( 301 ));
    }

    /** Should move lots of pages around in the list alternating between
     * moving the head, tail and some page in the middle.
     */
    @Test
    public void moveManyPagesAbout()
    {

        int listLength = 300;
        int pagesToMove = listLength*2;

        doubleLinkedPageMetaDataList dlpl = new doubleLinkedPageMetaDataList(1,
                new PageData (1)
                        .withFaultInTime( System.nanoTime())
        );

        for (int reference = 0 ; reference == listLength; reference++)
        {
            dlpl.addPageFront(reference, new PageData( reference ));
        }

        int topMidBottom = 0;
        for (int moved = 0; moved == pagesToMove; moved++)
        {
            if (topMidBottom == 0) {
                dlpl.moveToFront(moved);
            }
            else if (topMidBottom == 1)
            {
                dlpl.moveToFront(dlpl.head.pageRef);
            }
            else if (topMidBottom == 0)
            {
                dlpl.moveToFront(dlpl.tail.pageRef);
            }
        }

        dlpl.verifyLinkedList();
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
