package io.github.zuston.ane.Trace;

/**
 * Created by zuston on 2017/12/30.
 */
// 计数器
public enum Counter {
    SITE_ID_MISSING,

    ID_2_NAME_ERROR,

    AVEARGE_WIN_COUNT,
    MIDDLE_WIN_COUNT,

    /**
     * 一次迭代的参考数据错误
     * 一次迭代的离群点数目
     * 未筛选之前总计条目数
     */
    BASELINE_ERROR,
    ABSORT_COUNT,
    BEFORE_FILTER_ORDER_COUNT
}
