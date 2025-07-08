package site.hnfy258.demo4;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Arrays;

public class ConcurrentBlocklist {
    private ConcurrentHashMap<String,Boolean> blackList = new ConcurrentHashMap<>();

    // 默认构造函数
    public ConcurrentBlocklist() {
    }

    // 带初始黑名单的构造函数
    public ConcurrentBlocklist(List<String> initialItems) {
        for (String item : initialItems) {
            add(item);
        }
    }

    // 便利构造函数，支持可变参数
    public ConcurrentBlocklist(String... initialItems) {
        this(Arrays.asList(initialItems));
    }

    public boolean contains(String item){
        return blackList.containsKey(item);
    }

    public void add(String item){
        blackList.putIfAbsent(item,true);
    }

    // 添加移除方法
    public boolean remove(String item) {
        return blackList.remove(item) != null;
    }

    // 获取黑名单大小
    public int size() {
        return blackList.size();
    }

    public ConcurrentHashMap<String,Boolean> getBlackList(){
        return blackList;
    }
}
