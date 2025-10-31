package szp.rafael.cct.model;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;

public abstract class AbstractModel implements Serializable {

    private transient Gson gson =
            new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();


    public String toJSONString(){
        return gson.toJson(this);
    }

}
