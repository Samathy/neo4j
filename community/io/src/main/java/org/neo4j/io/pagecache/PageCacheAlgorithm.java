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
package org.neo4j.io.pagecache;

import org.neo4j.io.pagecache.impl.muninn.PageList;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;

import java.io.IOException;

public interface PageCacheAlgorithm
{

    /**
     * Find a page to  evict from the list of given pages.
     *
     * @param faultEvent
     * @param pages
     * @return long
     * @throws IOException
     */
    long cooperativlyEvict( PageFaultEvent faultEvent, PageList pages ) throws IOException;

    /** Notify the algorithm of a pin event and include some usful data such as the last usage time.
     *
     * @param pageRef
     * @param pageData
     */
    void notifyPin( long pageRef, PageData pageData);

    /** Notify the algorithm that we've evicted the page through some means
     * other than using it. Therefore it needs to update and internal references to
     * this given page.
     *
     * @param pageRef
     * @param pageData
     */
    void externalEviction( long pageRef, PageData pageData);
}

