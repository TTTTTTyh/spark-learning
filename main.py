from ReadData import Reader
import Tabu
from RTree import RTree,Rect
from SparkTabu import SparkTabu

if __name__ == "__main__":
    reader=Reader()
    cus_list=[ i for i in range(1,101)]
    para=Tabu.Para(weight_UB=200,dis=reader.dis,weight=[cus[3] for cus in reader.customer],\
        maxn_percent=0.7,cus=reader.customer)
    rtree = RTree(10,30)
    for cus in reader.customer:
        if cus[0] != 0:
            rect = Rect(cus[1],cus[1],cus[2],cus[2],cus[0])
            rtree.insert(rect)
    pare_dic,part_cnt=rtree.init_id()
    sparkTabu=SparkTabu(pare_dic,part_cnt,[i for i in range(1,101)],para)
    sparkTabu.search()