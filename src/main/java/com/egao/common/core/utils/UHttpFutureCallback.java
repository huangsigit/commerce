package com.egao.common.core.utils;

/**
 * Created by ant on 2015/3/26.
 */
public interface UHttpFutureCallback {

    public void completed(String content);

    public void failed(String err);

}
