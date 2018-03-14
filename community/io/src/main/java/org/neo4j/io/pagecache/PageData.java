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

import java.util.ArrayList;

/** Includes interesting data about a page to be passed to the
 * algorithm upon Pinning or faulting in.
 */
public class PageData
{
    private long pageRef;
    private long faultInTime;
    private long lastUsageTime;
    private long kSize = 1;
    long accessTimes[];

    public PageData( long pageRef )
    {
        this.pageRef = pageRef;
    }

    public PageData ( long pageRef, int kSize)
    {
        this.accessTimes = new long [kSize];
        this.kSize = kSize;
        this.pageRef = pageRef;
    }

    public PageData withFaultInTime( long faultInTime )
    {
        this.faultInTime = faultInTime;
        this.lastUsageTime = faultInTime;
        return this;
    }

    public PageData withLastUsage( long lastUsage )
    {
        this.lastUsageTime = lastUsage;
        return this;
    }

    public long getFaultInTime()
    {
        return this.faultInTime;
    }

    public long getLastUsageTime()
    {
        return this.lastUsageTime;
    }

    public long getHistoryTime( int k ) throws IndexOutOfBoundsException
    {
        //TODO should be protected with an if
        return this.accessTimes[k-1];
    }

    public void setAccessTime(int element, long time)
    {
            this.accessTimes[element] = time;
    }

    public long getPageRef()
    {
        return this.getPageRef();
    }
    public PageData withKSize( int kSize )
    {
        //Constructor for LRU-k algorithm where we store the last k
        //reference times of this page
        //This should only be called upon new page creation, really.
        this.accessTimes = new long [kSize];

        return this;
    }

}

