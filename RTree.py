from sys import maxsize as int_max
from queue import Queue
from turtle import left
from Tabu import Tabu
int_min = -int_max-1

class Rect:
    def __init__(self,xl=int_max,xu=int_min,yl=int_max,yu=int_min,id=-1) -> None:
        self.xl=xl
        self.xu=xu
        self.yl=yl
        self.yu=yu
        self.id=id
    
    def copy(self):
        copy=Rect(self.xl,self.xu,self.yl,self.yu)
        return copy

    def combine(self,rect)->None:
        self.xl=min(self.xl,rect.xl)
        self.xu=max(self.xu,rect.xu)
        self.yl=min(self.yl,rect.yl)
        self.yu=max(self.yu,rect.yu)

    def combine2(self,rect):
        newRect=Rect()
        newRect.combine(self)
        newRect.combine(rect)
        return newRect

    def get_square(self)->int:
        return (self.yu-self.yl)*(self.xu-self.xl)

class Node:
    def __init__(self,level=-1,is_leaf=False,child_cnt=0,rect:Rect=Rect()):
        self.level=level
        self.is_leaf=is_leaf
        self.child_cnt=child_cnt
        self.square=0
        self.children=list()
        self.rect=rect
    

def dfs_print(node:Node,need_print:list,is_end=True):
        for p in need_print:
            print(p,end='')
        if is_end:
            print('└──',end='')
        else:
            print('├──', end='')
        print(' ',node.rect.id)
        if len(node.children)==0:
            return
        else:
            cnt=len(node.children)
            for i,child_node in enumerate(node.children):
                add_print=' '*8 if is_end else '│'+' '*8
                need_print.append(add_print)
                is_i_end=i==cnt-1
                dfs_print(child_node,need_print,is_i_end)
                del need_print[-1]

class RTree:
    def __init__(self,max_cnt,min_cnt) -> None:
        self.max_cnt=max_cnt
        self.min_cnt=min_cnt
        self.m_root=Node()
        self.m_root.level=0 
     
    def __split_node__(self,cur_node:Node,new_node:Node)->None:
        index1= index2=-1
        max_waste=int_min
        for child in cur_node.children:
            child.square=child.rect.get_square()
        for i in range(0,len(cur_node.children)-1):
            for j in range(i+1,len(cur_node.children)):
                combine_square=cur_node.children[i].rect.combine2(cur_node.children[j].rect).get_square()
                s1=cur_node.children[i].square
                s2=cur_node.children[j].square
                waste=combine_square-s1-s2
                if waste > max_waste:
                    index1 = i 
                    index2 = j
                    max_waste=waste
        buffer=list(cur_node.children)
        cur_node.children.clear()
        cur_node.children.append(buffer[index1])
        cur_node.rect=buffer[index1].rect.copy()
        cur_node.square=cur_node.rect.get_square()
        cur_node.child_cnt=1
        new_node.children.clear()
        new_node.children.append(buffer[index2])
        new_node.rect=buffer[index2].rect.copy()
        new_node.square=new_node.rect.get_square()
        new_node.child_cnt=1
        del buffer[index2]
        del buffer[index1]
        while len(buffer)>0:
            pick_index=-1
            max_diff=int_min
            cur_square=cur_node.rect.get_square()
            new_square=new_node.rect.get_square()
            for i in range(len(buffer)):
                s_increase1=cur_node.rect.combine2(buffer[i].rect).get_square()-cur_square
                s_increase2=new_node.rect.combine2(buffer[i].rect).get_square()-new_square
                diff=abs(s_increase1-s_increase2)
                if diff > max_diff:
                    max_diff=diff
                    pick_index=i
                    pick_node= cur_node if s_increase1 < s_increase2 else new_node
            pick_node.children.append(buffer[pick_index])
            pick_node.rect.combine(buffer[pick_index].rect)
            pick_node.square=pick_node.rect.get_square()
            pick_node.child_cnt+=1
            del buffer[pick_index]
    
    def __choose_child_index__(self,cur_node:Node,rect:Rect)->Node:
        min_enlarge=int_max
        # assert (len(cur_node.children)>0)
        for child in cur_node.children:
            enlarge_rect=child.rect.combine2(rect)
            org_square=child.rect.get_square()
            enlarge_square=enlarge_rect.get_square()
            enlarge=enlarge_square-org_square
            if enlarge < min_enlarge:
                min_enlarge=enlarge
                return_val=child
        return return_val

    def __recursive_insert__(self,rect:Rect,cur_node:Node,new_node:Node)->bool:
        # assert(cur_node.level>=0)
        if cur_node.level == 0:
            node = Node(rect=rect,is_leaf=True)
            if cur_node.child_cnt < self.max_cnt:
                cur_node.children.append(node)
                cur_node.child_cnt+=1
                cur_node.rect.combine(rect)
                return False
            else:
                cur_node.children.append(node)
                cur_node.child_cnt+=1
                self.__split_node__(cur_node,new_node)
                return True
        else:
            # assert(cur_node.children[0].level !=-1)
            choose_child=self.__choose_child_index__(cur_node,rect)
            new_node_next_level=Node()
            is_split=self.__recursive_insert__(rect,choose_child,new_node_next_level)
            if is_split:
                if cur_node.child_cnt < self.max_cnt:
                    new_node_next_level.level=choose_child.level
                    cur_node.children.append(new_node_next_level)
                    cur_node.child_cnt+=1
                    cur_node.rect.combine(new_node_next_level.rect)
                    cur_node.square=cur_node.rect.get_square()
                    return False
                else:
                    new_node_next_level.level=choose_child.level
                    cur_node.children.append(new_node_next_level)
                    cur_node.child_cnt+=1
                    self.__split_node__(cur_node,new_node)
                    return True
            else:
                return False

    def insert(self,rect)->None:
        new_node_nxt_lev=Node()
        new_node_nxt_lev.level=self.m_root.level
        is_split=self.__recursive_insert__(rect,self.m_root,new_node_nxt_lev)
        if is_split:
            new_node_nxt_lev.level=self.m_root.level
            new_root=Node(child_cnt=2)
            new_root.children.extend([self.m_root,new_node_nxt_lev])
            new_root.rect=self.m_root.rect.copy()
            new_root.rect.combine(new_node_nxt_lev.rect)
            new_root.level=self.m_root.level+1
            self.m_root=new_root
        
    def init_id(self):
        pare_dic=dict()
        part_cnt=list()
        self.m_root.rect.id=-1        
        def bfs():
            nonlocal pare_dic,part_cnt
            no_leaf_child_id=-2
            q = Queue()
            q.put(self.m_root)
            while not q.empty():
                c_cnt=q.qsize()
                part_cnt.append(c_cnt)      
                for parent_part in range(c_cnt):
                    now=q.get()
                    if now.level==0:
                        for leaf in now.children:
                            pare_dic[int(leaf.rect.id)]=(parent_part,now.rect.id)
                    else:
                        for no_leaf in now.children:
                            assert(no_leaf.rect is not now.rect)
                            no_leaf.rect.id=no_leaf_child_id
                            no_leaf_child_id-=1
                            pare_dic[no_leaf.rect.id]=(parent_part,now.rect.id)
                            q.put(no_leaf)
        bfs()
        return pare_dic,part_cnt
    
    def print_tree(self):
        dfs_print(self.m_root,[],True)
        

    
        
            
                

