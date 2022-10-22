from curses import nonl
from math import ceil
from pyspark import SparkConf,SparkContext
import Tabu
class SparkTabu:
    def __init__(self,pare_dic:dict,part_cnt:list,cus_list:list,para:Tabu.Para) -> None:
        # conf=SparkConf().setAppName('sparkTabu').setMaster('yarn').set('spark.executor.instances','6').\
        #     set('spark.driver.extraJavaOptions',"-Dlog4j.configuration=file:log4j.properties").set("spark.yarn.jars",'hdfs://master/spark/jars/*')
        
        conf=SparkConf().setAppName('sparkTabu').setMaster('local[*]').\
            set('spark.driver.extraJavaOptions',"-Dlog4j.configuration=file:log4j.properties")
        conf.set("spark.submit.pyFiles", "Tabu.py")  
        sc=SparkContext(conf=conf)
        sc.setLogLevel('WARN')
        self.sc=sc            
        bc_pare_dic=sc.broadcast(pare_dic)
        self.part_num=part_cnt[-1]
        cus_rdd=sc.parallelize(cus_list).map(lambda x:[bc_pare_dic.value[x][0],x])
        cus_rdd=cus_rdd.partitionBy(part_cnt.pop(),lambda k:k).glom()
        def build_tabu(x):
            self_id=bc_pare_dic.value[x[0][1]][1]
            parent_id=bc_pare_dic.value[self_id][1]
            tabu=Tabu.Tabu([ i[1] for i in x],ceil(len(x)/10),15,parent_id,self_id,para)
            tabu.part_index=bc_pare_dic.value[x[0][1]][0]
            return tabu
        self.tabu_rdd=cus_rdd.map(build_tabu)
        self.bc_pare_dic=bc_pare_dic
        self.part_cnt=part_cnt
   
    def search(self):
        pare_dic=self.bc_pare_dic
        now_max_iters=[-1,2000,100,100]
        now_max_iter=now_max_iters.pop()
        def tabu_search_with_init(tabu):            
            init_routes=tabu.init_solution()            
            tabu.search(init_routes,now_max_iter)
            tabu=(pare_dic.value[tabu.id][0],tabu)
            return tabu
        def tabu_search(x):
            tabu=x[1]
            tabu.search(tabu.best_routes,now_max_iter)
            tabu=(tabu.id,tabu)
            return tabu
        def pre_combine(x):
            x[1].combine_ids=[x[1].part_index]
            return x

        def combine(a,b):
            a.combine(b)
            a.combine_ids.append(b.part_index)
            return a     
        def after_combine(x):
            grandpa_id=pare_dic.value.get(x[1].parent_id,(None,None))[1]
            x[1].id=x[1].parent_id
            x[1].parent_id=grandpa_id
            return x   
        def copy(x):
            x=x[1]
            part_info_iter=iter(x.combine_ids)
            first_part=next(part_info_iter)
            l=[(first_part,x)]
            x.part_index=first_part
            for part in part_info_iter:
                x_copy=x.copy()
                x_copy.part_index=part
                l.append((part,x_copy))
            return l           
        def find_min(a,b):
            if a.best_cost<b.best_cost:
                return a
            else:
                return b
  

        self.tabu_rdd= self.tabu_rdd.map(tabu_search_with_init)        
        while len(self.part_cnt)>0:
            self.tabu_rdd=self.tabu_rdd.map(pre_combine).reduceByKey(combine).map(after_combine).flatMap(copy).partitionBy(self.part_num,lambda k:k)       
            self.tabu_rdd=self.tabu_rdd.map(tabu_search).reduceByKey(find_min).map(lambda x:(pare_dic.value.get(x[0],(None,None))[0],x[1]))
            self.part_cnt.pop()
            now_max_iter=now_max_iters.pop()

        print(self.tabu_rdd.map(lambda x:x[1]).collect())
        print('over')



        


