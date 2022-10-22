from RTree import RTree, Rect
from ReadData import Reader
import numpy as np
import Tabu


def flat(l: list):
    it = iter(l)
    first = next(it)
    if type(first) is list:
        first = flat(first)
    else:
        first = [first]
    for n in it:
        if type(n) is list:
            first += flat(n)
        else:
            first.append(n)
    return first


def partition(rdd: list):
    dic = dict()
    for r in rdd:
        if dic.get(r[0]) == None:
            dic[r[0]] = [r[1]]
        else:
            dic[r[0]].append(r[1])
    for k in dic:
        it = iter(dic[k])
        first = next(it)
        for n in it:
            first = combine(first, n)
        dic[k] = first
    rdd = [(k, dic[k]) for k in dic]
    return rdd


if __name__ == '__main__':
    reader = Reader()
    cus_list = [i for i in range(1, 101)]
    para=Tabu.Para(weight_UB=200,dis=reader.dis,weight=[cus[3] for cus in reader.customer],\
        maxn_percent=0.7,cus=reader.customer)
    rtree = RTree(5, 10)
    for cus in reader.customer:
        if cus[0] != 0:
            rect = Rect(cus[1], cus[1], cus[2], cus[2], cus[0])
            rtree.insert(rect)
    pare_dic, part_cnt = rtree.init_id()
    cus_list = [i for i in range(1, 101)]

    def build_tabu(x):
        tabu = Tabu.Tabu(x, np.ceil(len(x) / 10), 15, pare_dic[x[0]][1], para)
        tabu.part_index = pare_dic[x[0]][0]
        return tabu

    rdd = [[] for _ in range(27)]
    for cus in cus_list:
        rdd[pare_dic[cus][0]].append(cus)
    for i, cus in enumerate(rdd):
        rdd[i] = build_tabu(cus)

    now_max_iter = 100

    def tabu_search_with_init(tabu):
        init_routes = tabu.init_solution()
        tabu.search(init_routes, now_max_iter)
        tabu = (pare_dic[tabu.parent_part][0], tabu)
        return tabu

    def tabu_search(x):
        tabu = x[1]
        tabu.search(tabu.best_routes, now_max_iter)
        if tabu.parent_part in pare_dic:
            tabu = (pare_dic[tabu.parent_part][0], tabu)
        else:
            tabu = (0, tabu)
        return tabu

    def pre_combine(x):
        x[1].combine_ids = [x[1].part_index]
        return x

    def combine(a, b):
        a.combine(b)
        a.combine_ids.append(b.part_index)
        return a

    def after_combine(x):
        x[1].parent_part = pare_dic[x[1].parent_part][1]
        return x

    def copy(x):
        x = x[1]
        part_info_iter = iter(x.combine_ids)
        first_part = next(part_info_iter)
        l = [(first_part, x)]
        x.part_index = first_part
        for part in part_info_iter:
            x_copy = x.copy()
            x_copy.part_index = part
            l.append((part, x_copy))
        return l

    for i, cus in enumerate(rdd):
        rdd[i] = tabu_search_with_init(cus)
    part_cnt.pop()
    while len(part_cnt) > 0:
        for i, cus in enumerate(rdd):
            rdd[i] = pre_combine(cus)
        rdd = partition(rdd)
        for i, cus in enumerate(rdd):
            rdd[i] = after_combine(cus)
        for i, cus in enumerate(rdd):
            rdd[i] = copy(cus)
        rdd = flat(rdd)
        rdd = partition(rdd)
        for i, cus in enumerate(rdd):
            rdd[i] = tabu_search(cus)
        part_cnt.pop()
        now_max_iter=max(now_max_iter//10,10)
    a = 1
