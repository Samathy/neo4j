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

/** Includes interesting data about a page to be passed to the
 * algorithm upon Pinning or faulting in.
 */
public class PageData
{
    private long pageRef;
    private long faultInTime;
    private long lastUsageTime;
    private long kSize = 1;
    private long[] accessTimes; //Should be private
    private long references;

    private boolean isNew = true;
    private boolean isOld = false;

    public PageData( long pageRef )
    {
        this.pageRef = pageRef;
    }

    public PageData( long pageRef, int kSize )
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
        if ( this.accessTimes == null )
        {
            throw new IndexOutOfBoundsException();
        }

        if ( k - 1 < 0 || k > accessTimes.length )
        {
            throw new IndexOutOfBoundsException();
        }

        if ( k == 1 )
        {
            return this.lastUsageTime;
        }

        //TODO should be protected with an if
        return this.accessTimes[k - 1];
    }

    public synchronized void setAccessTime( int element, long time )
    {
            this.accessTimes[element - 1] = time;
    }

    /** Sets last usage time of this page data
     *
     * IMPORTANT
     * Differs from setAccessTime because this method
     * does NOT also update the history variable.
     * It is used in setting the last access time without setting the
     * history variable.
     * @param time
     */
    public synchronized void setLastUsageTime( long time )
    {

    }

    public long getPageRef()
    {
        return this.pageRef;
    }
    public PageData withKSize( int kSize )
    {
        //Constructor for LRU-k algorithm where we store the last k
        //reference times of this page
        //This should only be called upon new page creation, really.
        this.accessTimes = new long [kSize];

        return this;
    }

    public synchronized void setNew( boolean val )
    {
        this.isNew = val;
    }

    public boolean isNew()
    {
        return this.isNew;
    }

    public synchronized void setOld( boolean val )
    {
        this.isOld = val;
    }

    public boolean isOld()
    {
        return this.isOld;
    }

    public synchronized void incReferences()
    {
        this.references++;
    }

    public synchronized void decReferences()
    {
        if ( this.references != 0 )
        {
            this.references--;
        }
        else
        {
            System.out.println( "Should not be reducing reference count below 0!" );
        }
    }

    public long getRefCount()
    {
        return this.references;
    }

    public synchronized void setRefCount( long refCount )
    {
        this.references = refCount;
    }

}

