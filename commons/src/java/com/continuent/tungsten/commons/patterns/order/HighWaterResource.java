
package com.continuent.tungsten.commons.patterns.order;

public class HighWaterResource implements Comparable<HighWaterResource>
{
    private long                        highWaterEpoch    = -1;
    private String                      highWaterEventId  = "";

    // New event ids are in the form <log file #>:<offset>;<sessionId>
    private static final String SESSION_DELIMITER = ";";

    public HighWaterResource()
    {

    }

    public HighWaterResource(long epoch, String eventId)
    {
        this.highWaterEpoch = epoch;
        this.highWaterEventId = eventId;
    }

    public HighWaterResource(String resourceAsString)
    {
        String epochStr = resourceAsString.substring(0, resourceAsString
                .indexOf('('));
        this.highWaterEpoch = Long.valueOf(epochStr);
        this.highWaterEventId = resourceAsString.substring(resourceAsString
                .indexOf('(') + 1, resourceAsString.length() - 1);
    }

    // We are comparing ourselves to what is passed in.
    public int compareTo(HighWaterResource o)
    {
        if (this.highWaterEpoch > o.getHighWaterEpoch())
            return 1;
        else if (this.highWaterEpoch < o.getHighWaterEpoch())
            return -1;
        else
        {
            String oToCompare = o.getHighWaterEventId();
            String thisToCompare = this.highWaterEventId;
            int sessionDelimiter;

            if ((sessionDelimiter = o.highWaterEventId
                    .indexOf(SESSION_DELIMITER)) != -1)
            {
                oToCompare = o.getHighWaterEventId().substring(0, sessionDelimiter);
            }
            else
            {
                oToCompare = o.getHighWaterEventId();
            }
            if ((sessionDelimiter = this.highWaterEventId
                    .indexOf(SESSION_DELIMITER)) != -1)
            {
                thisToCompare = this.highWaterEventId.substring(0,
                        sessionDelimiter);
            }
            else
            {
                thisToCompare = this.highWaterEventId;
            }
            
            if (oToCompare.length() == 0 && thisToCompare.length() > 0)
            {
                return 1;
            }
            else if (thisToCompare.length() == 0 && oToCompare.length() > 0)
            {
                return -1;
            }
            
            return (thisToCompare.compareTo(oToCompare));

        }
    }

    public void update(long epoch, String eventId)
    {
        this.highWaterEpoch = epoch;
        this.highWaterEventId = eventId;
    }

    public String toString()
    {
        return String.format("%d(%s)", highWaterEpoch, highWaterEventId);
    }

    public static String getSessionId(String eventId)
    {
        int sessionDelimiter;

        if ((sessionDelimiter = eventId.indexOf(SESSION_DELIMITER)) != -1)
        {
            return eventId.substring(sessionDelimiter + 1);
        }

        return null;
    }

    public long getHighWaterEpoch()
    {
        return highWaterEpoch;
    }

    public void setHighWaterEpoch(long highWaterEpoch)
    {
        this.highWaterEpoch = highWaterEpoch;
    }

    public String getHighWaterEventId()
    {
        return highWaterEventId;
    }

    public void setHighWaterEventId(String highWaterEventId)
    {
        this.highWaterEventId = highWaterEventId;
    }

}
