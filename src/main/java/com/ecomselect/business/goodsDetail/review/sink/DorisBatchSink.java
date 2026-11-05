package com.ecomselect.business.goodsDetail.review.sink;

import com.ecomselect.business.common.model.review.DWDReviewRecord;
import com.ecomselect.business.common.model.review.ODSReviewRecord;
import org.apache.doris.flink.sink.DorisSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Doris批量写入Sink - Review适配器。
 */
public class DorisBatchSink {

    private static final AtomicLong odsCounter = new AtomicLong(0);
    private static final AtomicLong dwdCounter = new AtomicLong(0);

    private static final String ODS_REVIEW_TABLE = "ods_review_raw";
    private static final String DWD_REVIEW_TABLE = "dwd_review_base";

    public static DorisSink<String> createODSDorisSink() {
        return com.ecomselect.business.common.sink.DorisBatchSink.createODSDorisSink(ODS_REVIEW_TABLE);
    }

    public static DorisSink<String> createDWDDorisSink() {
        return com.ecomselect.business.common.sink.DorisBatchSink.createDWDDorisSink(DWD_REVIEW_TABLE);
    }

    public static String odsRecordToJson(ODSReviewRecord record) {
        Function<ODSReviewRecord, String> logExtractor = r -> String.format("评论ID=%s, 商品ID=%s, 站点ID=%s",
                r.getReviewId(), r.getGoodsId(), r.getSiteId());
        String json = com.ecomselect.business.common.sink.DorisBatchSink.recordToJson(
                record, ODSReviewRecord.class, odsCounter, logExtractor);
        return json;
    }

    public static String dwdRecordToJson(DWDReviewRecord record) {
        Function<DWDReviewRecord, String> logExtractor = r -> String.format("评论ID=%s, 商品ID=%s, 站点ID=%s",
                r.getReviewId(), r.getGoodsId(), r.getSiteId());
        String json = com.ecomselect.business.common.sink.DorisBatchSink.recordToJsonByReflection(
                record, DWDReviewRecord.class, dwdCounter, logExtractor);
        return json;
    }
}
