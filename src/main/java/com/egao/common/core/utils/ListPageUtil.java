package com.egao.common.core.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 常用工具方法
 * Created by hs on 2017-6-10 10:10
 */


public class ListPageUtil<T> {
    private List<T> data;

    /** 上一页 */
    private int lastPage;

    /** 当前页 */
    private int currentPage;

    /** 下一页 */
    private int nextPage;
//
    /** 每页条数 */
    private int pageSize;

    /** 总页数 */
    private int totalPage;

    /** 总数据条数 */
    private int totalCount;

    public ListPageUtil(List<T> data,int currentPage,int pageSize) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("data must be not empty!");
        }

        this.data = data;
        this.pageSize = pageSize;
        this.currentPage = currentPage;
        this.totalCount = data.size();
        this.totalPage = (totalCount + pageSize - 1) / pageSize;
        this.lastPage = currentPage-1>1? currentPage-1:1;
        this.nextPage = currentPage>=totalPage? totalPage: currentPage + 1;

    }


    public int getPageSize() {
        return pageSize;
    }

    public List<T> getData() {
        int fromIndex = (currentPage - 1) * pageSize;
        System.out.println("9999999999999fromIndex："+fromIndex);
        System.out.println("currentPage："+currentPage);
        System.out.println("pageSize："+pageSize);
        if (fromIndex >= data.size()) {
            System.out.println("空数组1："+data.size());
            return Collections.emptyList();//空数组
        }
        if(fromIndex<0){
            System.out.println("空数组2："+fromIndex);
            return Collections.emptyList();//空数组

        }
        int toIndex = currentPage * pageSize;
        System.out.println("toIndex："+toIndex);
        if (toIndex >= data.size()) {
            toIndex = data.size();
        }
        System.out.println("data.subList(fromIndex, toIndex)："+data.subList(fromIndex, toIndex));
        return data.subList(fromIndex, toIndex);
    }

    public int getLastPage() {
        return lastPage;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public int getNextPage() {
        return nextPage;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public int getTotalCount() {
        return totalCount;
    }

    @Override
    public String toString() {
        return "{" +
                "\"data\":" + data +
                ", \"lastPage\":" + lastPage +
                ", \"currentPage\":" + currentPage +
                ", \"nextPage\":" + nextPage +
                ", \"pageSize\":" + pageSize +
                ", \"totalPage\":" + totalPage +
                ", \"totalCount\":" + totalCount +
                '}';
    }
}
