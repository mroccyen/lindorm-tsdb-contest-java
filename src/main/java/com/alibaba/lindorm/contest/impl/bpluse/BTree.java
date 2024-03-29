package com.alibaba.lindorm.contest.impl.bpluse;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public class BTree<K extends Comparable<K>> {
    // m阶B树
    private final byte m;
    private BTNode root;
    private BTNode sqt;

    public BTree(byte m) {
        if (m < 3) {
            throw new ExceptionInInitializerError("m is too small: at least 3");
        }
        this.m = m;
        root = new BTNode(m);
        sqt = root;
    }

    /**
     * 范围查询
     *
     * @param keyBegin 范围开始
     * @param keyEnd   范围结束
     * @return 结果
     */
    public List<Object> searchRange(K keyBegin, K keyEnd) {
        List<Object> r = new ArrayList<>();
        Result result = searchKeyInternal(keyBegin);
        if (!result.isTag() && keyBegin.compareTo(keyEnd) == 0) {
            return new ArrayList<>();
        } else if (result.isTag() && keyBegin.compareTo(keyEnd) == 0) {
            r.add(result.pt.getPtrs()[result.getIndex()]);
            return r;
        } else {
            boolean hasFind = false;
            for (int i = result.index; i < result.pt.getSize(); i++) {
                K k = (K) result.pt.keys[i];
                if (k.compareTo(keyBegin) >= 0 && k.compareTo(keyEnd) < 0) {
                    r.add(result.pt.getPtrs()[i]);
                } else {
                    hasFind = true;
                    break;
                }
            }
            if (!hasFind) {
                BTNode next = result.getPt().next;
                while (!hasFind) {
                    if (next == null) {
                        break;
                    }
                    for (int i = 0; i < next.getSize(); i++) {
                        K key = (K) next.getKeys()[i];
                        if (key.compareTo(keyBegin) >= 0 && key.compareTo(keyEnd) < 0) {
                            r.add(next.getPtrs()[i]);
                        } else {
                            hasFind = true;
                            break;
                        }
                    }
                    next = next.next;
                }
            }
        }
        return r;
    }

    /**
     * 查询最大key
     *
     * @param key key
     * @return 结果
     */
    public Object searchMax(K key) {
        Result result = searchKeyInternal(key);
        return result.getPt().getMaxPtr();
    }

    /**
     * 插入key 和 value 到树中
     *
     * @param key   关键字
     * @param value 卫星数据
     */
    public boolean insert(K key, Object value) {
        RBranchRef ref = new RBranchRef();
        return insertAndAdjust(null, -1, root, key, value, ref);
    }

    /**
     * 打印树结构
     */
    public void print() {
        print(root, 0);
    }

    /**
     * 顺序遍历keys
     */
    public void printKeysInorder() {
        BTNode p = sqt;
        K last = null;
        while (p != null) {
            for (int i = 0; i < p.size; ++i) {
                K k = (K) p.keys[i];
                System.out.print(k + " ");
                if (last != null && last.compareTo(k) > 0) {
                    throw new Error("B树错误: 元素不是有序的！");
                }
                last = k;
            }
            p = p.next;
        }
    }

    /**
     * 插入B树中的某个key: 删除成功返回true
     *
     * @param key 关键字
     */
    public boolean delete(K key) {
        MergeFlag flag = new MergeFlag();
        return deleteAndAdjust(null, -1, root, key, flag);
    }

    /*---------------------------------------------------------⬇️私有方法⬇️-----------------------------------------------*/

    /**
     * 分支引用类,用来传递待插入的分支引用(作用相当于C++里的引用)
     */
    private static class RBranchRef {
        Object rbranch = null;
    }

    /**
     * 递归插入key, value 并调整
     *
     * @param parent 当前结点的父亲
     * @param pindex 当前结点在父亲中的索引下标
     * @param T      当前结点
     * @param key    待插入关键字
     * @param value  待插入的卫星数据
     * @param ref    待插入的关键字对应的分支
     */
    private boolean insertAndAdjust(BTNode parent, int pindex, BTNode T, K key, Object value, RBranchRef ref) {
        int i = lowerBound(T, key);
        if (i == T.size) {
            --i;
        }
        // 向下递归插入
        if (!T.isleaf) {
            boolean insertFlag = insertAndAdjust(T, i, (BTNode) T.ptrs[i], key, value, ref);
            if (!insertFlag) {
                // 数据已经存在, 不需要插入
                return false;
            }
        } else {
            // 卫星数据已存在, 更新
            if (T.size > 0 && key.compareTo((K) (T.keys[i])) == 0) {
                T.ptrs[i] = value;
                return false;
            }
        }

        // 向上调整
        K curKey = null;
        if (T.isleaf) {
            curKey = key;
            ref.rbranch = value;
        } else if (ref.rbranch != null) {
            curKey = (K) ((BTNode) (ref.rbranch)).getMaxKey();
        }
        // 插入
        if (curKey != null) {
            if (T.size >= m) {
                insertAndRSplit(T, curKey, ref);
            } else {
                insert(T, curKey, ref);
            }
        }

        // 更新父亲key结点
        if (T == root) {
            // 根结点长高一层
            if (ref.rbranch != null) {
                root = new BTNode(m);
                root.isleaf = false;
                root.size = 2;
                root.keys[0] = T.getMaxKey();
                root.keys[1] = ((BTNode) (ref.rbranch)).getMaxKey();
                root.ptrs[0] = T;
                root.ptrs[1] = ref.rbranch;
            }
        } else {
            // 更新父亲key结点
            parent.keys[pindex] = T.getMaxKey();
        }

        return true;
    }

    /**
     * 插入并向右分裂
     *
     * @param T      当前结点
     * @param curKey 插入的Key
     * @param ref    插入的分支
     */
    private void insertAndRSplit(BTNode T, K curKey, RBranchRef ref) {
        BTNode rb = new BTNode(m);
        insert(T, curKey, ref);
        int splitpos = (int) Math.ceil((double) m / 2.0);
        for (int i = splitpos, k = 0; i < T.size; ++i, ++k) { // 分裂
            rb.keys[k] = T.keys[i];
            rb.ptrs[k] = T.ptrs[i];
            T.keys[i] = T.ptrs[i] = null; // 剪掉分支
        }
        rb.size = T.size - splitpos;
        T.size = splitpos;
        rb.isleaf = T.isleaf;
        ref.rbranch = rb;

        // 更新叶结点之间的指针（双向链表）
        if (T.isleaf) {
            rb.prev = T;
            rb.next = T.next;
            if (T.next != null) {
                T.next.prev = rb;
            }
            T.next = rb;
        }
    }

    /**
     * 简单插入
     *
     * @param T      当前结点
     * @param curKey 插入的Key
     * @param ref    插入的分支
     */
    private void insert(BTNode T, K curKey, RBranchRef ref) {
        int i = lowerBound(T, curKey);
        for (int j = T.size - 1; j >= i; --j) {
            T.ptrs[j + 1] = T.ptrs[j];
            T.keys[j + 1] = T.keys[j];
        }
        T.ptrs[i] = ref.rbranch;
        T.keys[i] = curKey;
        T.size++;
        ref.rbranch = null;
    }

    /**
     * 在B树上查找关键字key,返回 Result(pt, index, tag)。若查找成功，则特征值tag 为 true, pt.keys[index] 等于 key；
     * 否则tag = false, 等于key的关键字应该插入在 pt.keys[index-1] 和 pt.keys[index]之间。
     *
     * @param key 关键字
     * @return 结果
     */
    private Result searchKeyInternal(K key) {
        BTNode p = root;
        int index = 0;
        while (!p.isleaf) {
            index = lowerBound(p, key);
            if (index >= p.size) {
                index = p.size - 1;
            }
            p = (BTNode) p.ptrs[index];
        }
        index = lowerBound(p, key);
        if (p.size > 0 && index < p.getSize() && key.compareTo((K) p.keys[index]) == 0) {
            return new Result(p, index, true);
        } else {
            return new Result(p, index, false);
        }
    }

    /**
     * 返回第一个>=key的位置,如果找不到则返回最后一个元素的位置
     *
     * @param T   结点
     * @param key 关键字
     * @return 位置
     */
    private int lowerBound(BTNode T, K key) {
        int low = 0;
        int high = T.size;
        while (low < high) {
            int mid = (low + high) / 2;
            K k = (K) T.keys[mid];
            int cmp = key.compareTo(k);
            if (cmp <= 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    /**
     * 递归打印树结构
     *
     * @param T     当前树
     * @param level 第几层
     */
    private void print(BTNode T, int level) {
        if (T != null) {
            for (int i = 0; i < level; ++i) {
                System.out.print("\t");
            }
            System.out.printf("%s\n", T);
            if (!T.isleaf) {
                for (int i = 0; i < T.size; ++i) {
                    BTNode t = (BTNode) T.ptrs[i];
                    print(t, level + 1);
                }
            }
        }
    }

    /**
     * 合并标志类
     */
    private static class MergeFlag {
        boolean left = false;
        boolean right = false;
    }

    /**
     * 简单删除
     *
     * @param T    当前树
     * @param i    待删除key的下标
     * @param flag 合并标志
     */
    private void delete(BTNode T, int i, MergeFlag flag) {
        for (int j = i; j < T.size - 1; ++j) {
            T.keys[j] = T.keys[j + 1];
            T.ptrs[j] = T.ptrs[j + 1];
        }
        T.keys[T.size - 1] = T.ptrs[T.size - 1] = null;
        T.size--;
        flag.left = flag.right = false;
    }

    /**
     * 删除并借走左兄弟一个元素
     *
     * @param parent 父亲
     * @param pindex 父亲中当前结点的索引下标
     * @param T      当前结点
     * @param index  待删除的关键字下标
     * @param flag   合并标志
     */
    private void deleteAndBorrowLeft(BTNode parent, int pindex, BTNode T, int index, MergeFlag flag) {
        delete(T, index, flag);
        BTNode lBrother = (BTNode) parent.ptrs[pindex - 1];
        for (int j = T.size - 1; j >= 0; --j) {
            T.keys[j + 1] = T.keys[j];
            T.ptrs[j + 1] = T.ptrs[j];
        }
        int li = lBrother.size - 1;
        T.keys[0] = lBrother.keys[li];
        T.ptrs[0] = lBrother.ptrs[li];
        T.size++;
        lBrother.keys[li] = lBrother.ptrs[li] = null;
        lBrother.size--;
    }

    /**
     * 删除并借走右兄弟一个元素
     *
     * @param parent 父亲
     * @param pindex 父亲中当前结点的索引下标
     * @param T      当前结点
     * @param index  待删除的关键字下标
     * @param flag   合并标志
     */
    private void deleteAndBorrowRight(BTNode parent, int pindex, BTNode T, int index, MergeFlag flag) {
        delete(T, index, flag);
        BTNode rBrother = (BTNode) parent.ptrs[pindex + 1];
        T.keys[T.size] = rBrother.keys[0];
        T.ptrs[T.size] = rBrother.ptrs[0];
        T.size++;
        for (int j = 0; j < rBrother.size - 1; ++j) {
            rBrother.keys[j] = rBrother.keys[j + 1];
            rBrother.ptrs[j] = rBrother.ptrs[j + 1];
        }

        int ri = rBrother.size - 1;
        rBrother.keys[ri] = rBrother.ptrs[ri] = null;
        rBrother.size--;
    }

    /**
     * 删除并合并至左兄弟
     *
     * @param parent 父亲
     * @param pindex 父亲中当前结点的索引下标
     * @param T      当前结点
     * @param index  待删除的关键字下标
     * @param flag   合并标志
     */
    private void deleteAndMergeToLeft(BTNode parent, int pindex, BTNode T, int index, MergeFlag flag) {
        delete(T, index, flag);
        BTNode lBrother = (BTNode) parent.ptrs[pindex - 1];
        for (int i = lBrother.size, j = 0; j < T.size; ++j, ++i) {
            lBrother.keys[i] = T.keys[j];
            lBrother.ptrs[i] = T.ptrs[j];
            T.keys[j] = T.ptrs[j] = null;
        }
        lBrother.size += T.size;
        flag.left = true;
    }

    /**
     * 删除并合并至右兄弟
     *
     * @param parent 父亲
     * @param pindex 父亲中当前结点的索引下标
     * @param T      当前结点
     * @param index  待删除的关键字下标
     * @param flag   合并标志
     */
    private void deleteAndMergeToRight(BTNode parent, int pindex, BTNode T, int index, MergeFlag flag) {
        delete(T, index, flag);
        BTNode rBrother = (BTNode) parent.ptrs[pindex + 1];
        for (int j = rBrother.size - 1; j >= 0; --j) {
            rBrother.keys[j + T.size] = rBrother.keys[j];
            rBrother.ptrs[j + T.size] = rBrother.ptrs[j];
        }
        rBrother.size += T.size;
        for (int j = 0; j < T.size; ++j) {
            rBrother.keys[j] = T.keys[j];
            rBrother.ptrs[j] = T.ptrs[j];
            T.keys[j] = T.ptrs[j] = null;
        }
        flag.right = true;
    }

    /**
     * 递归删除B树中的key和它所对应的值
     *
     * @param parent 当前结点父亲
     * @param pindex 当前结点在父结点中的索引下标
     * @param T      当前结点
     * @param key    待删除关键字
     * @param flag   合并标志
     * @return 删除成功
     */
    private boolean deleteAndAdjust(BTNode parent, int pindex, BTNode T, K key, MergeFlag flag) {
        int i = lowerBound(T, key);
        if (i >= T.size) {
            return false;
        }
        // 向下递归删除
        if (!T.isleaf) {
            boolean deleteFlag = deleteAndAdjust(T, i, (BTNode) T.ptrs[i], key, flag);
            if (!deleteFlag) {
                return false;// 数据不存在, 删除失败
            }
        } else {
            // 待删除的卫星数据不存在,删除失败
            if (key.compareTo((K) (T.keys[i])) != 0) {
                return false;
            }
        }

        // 向上调整
        // 删除
        if (T.isleaf || flag.left || flag.right) {
            int low = (int) Math.ceil((double) m / 2.0);
            // 直接删除
            if (T == root || T.size > low) {
                delete(T, i, flag);
            } else {
                // 下溢
                int psize = parent.size;
                boolean borrowed = false;
                BTNode lBrother = null, rBrother = null;

                // 向兄弟借key
                if (pindex > 0) { // 尝试借左兄弟
                    lBrother = (BTNode) parent.ptrs[pindex - 1];
                    if (lBrother.size > low) {
                        deleteAndBorrowLeft(parent, pindex, T, i, flag);
                        borrowed = true;
                        // 调整父结点
                        parent.keys[pindex - 1] = lBrother.getMaxKey();
                    }
                } else if (pindex < psize - 1) { // 尝试借右兄弟
                    rBrother = (BTNode) parent.ptrs[pindex + 1];
                    if (rBrother.size > low) {
                        deleteAndBorrowRight(parent, pindex, T, i, flag);
                        borrowed = true;
                        // 调整父结点
                        parent.keys[pindex] = T.getMaxKey();
                    }

                }
                // 借兄弟不成,则合并至兄弟
                if (!borrowed) {
                    if (lBrother != null) {
                        deleteAndMergeToLeft(parent, pindex, T, i, flag);
                        // 调整父结点
                        parent.keys[pindex - 1] = lBrother.getMaxKey();
                        // 调整叶子链表
                        if (T.isleaf) {
                            lBrother.next = T.next;
                            if (T.next != null) {
                                T.next.prev = lBrother;
                            }
                            T.next = T.prev = null;
                        }
                    } else if (rBrother != null) {
                        deleteAndMergeToRight(parent, pindex, T, i, flag);
                        // 调整父结点
                        //parent.keys[pindex + 1] = rBrother.getMaxKey();
                        // 调整叶子链表
                        if (T.isleaf) {
                            rBrother.prev = T.prev;
                            if (T.prev != null) {
                                T.prev.next = rBrother;
                            } else {
                                sqt = rBrother;
                            }
                            T.next = T.prev = null;
                        }
                    }
                }
            }
        }

        // 如果根处下溢
        if (T == root && !T.isleaf && T.size <= 1) {
            // 根结点降低一层
            root = (BTNode) T.ptrs[0];
            T.keys[0] = T.ptrs[0] = null;
        }
        return true;
    }
}
