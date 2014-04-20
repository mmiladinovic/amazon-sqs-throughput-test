import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: miroslavmiladinovic
 * Date: 19/04/2014
 * Time: 19:52
 * To change this template use File | Settings | File Templates.
 */
@ThreadSafe
public class SimpleMessage implements Serializable {

    private static final long serialVersionUID = 1896180568594719315L;

    private final String objectId;
    private final String userId;

    public SimpleMessage(String objectId, String userId) {
        this.objectId = objectId;
        this.userId = userId;
    }

    @JsonProperty
    public String getUserId() {
        return userId;
    }

    @JsonProperty
    public String getObjectId() {
        return objectId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((objectId == null) ? 0 : objectId.hashCode());
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleMessage other = (SimpleMessage) obj;
        if (objectId == null) {
            if (other.objectId != null)
                return false;
        } else if (!objectId.equals(other.objectId))
            return false;
        if (userId == null) {
            if (other.userId != null)
                return false;
        } else if (!userId.equals(other.userId))
            return false;
        return true;
    }

}
