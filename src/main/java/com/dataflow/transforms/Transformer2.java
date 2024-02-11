package com.dataflow.transforms;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Transformer2 extends DoFn<String, String> {

    public static TupleTag<String> successTag = new TupleTag<String>() {};

    // TupleTag for Error scenarios
    public static TupleTag<String> failureTag = new TupleTag<String>() {};

    private static final Logger LOG = LoggerFactory.getLogger(Transformer2.class);

    @ProcessElement
    public void processElement(@Element String record, MultiOutputReceiver out) {
        String className = this.getClass().getName()
                .substring(this.getClass().getName().lastIndexOf('.') + 1);
        LOG.info("Inside Transform {} ", className);
        LOG.info(record);



        try {
            Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
                    .disableHtmlEscaping().create();

            Map<String, Object> inputMap = gson.fromJson(record, ConcurrentHashMap.class);
            inputMap.put(className, "success");

            if (inputMap != null && !inputMap.isEmpty()) {
                out.get(successTag).output(gson.toJson(inputMap));
            }
        } catch (Exception e) {
            LOG.error("Error : {}",  e);
            out.get(failureTag).output("Error in + " + this.getClass().getName());
        }
    }
}
