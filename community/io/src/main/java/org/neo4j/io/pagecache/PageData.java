package org.neo4j.io.pagecache;

/** Includes interesting data about a page to be passed to the
 * algorithm upon Pinning or faulting in.
 */
public class PageData
{
    long pageRef = 0;
    long faultInTime = 0;
    long lastUsageTime = 0;

    public PageData ( long pageRef )
    {
        this.pageRef = pageRef;
    }

    public PageData withFaultInTime ( long faultInTime)
    {
        this.faultInTime = faultInTime;
        return this;
    }

    public PageData withLastUsage ( long lastUsage)
    {
        this.lastUsageTime = lastUsage;
        return this;
    }
}

