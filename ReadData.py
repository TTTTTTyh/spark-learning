import numpy as np
class Reader:
    def read_points(self):
        with open('data.csv','r',encoding='utf-8') as f:
            f.readline()
            lines = f.readlines()
            points=np.zeros([len(lines),7])
            for line in lines:
                cells=line.split(',')
                id=int(cells[0])
                for i in range(0,7):
                    points[id][i]=int(cells[i])
        return points,points.shape[0]

    def read_dis(self,points_count:int)->np.array:
        dis_mat=np.zeros([points_count,points_count])
        with open('dis.csv','r',encoding='utf-8') as f:
            lines = f.readlines()
            i=0
            for line in lines:
                cells=line.split(',')
                for j in range(0,len(cells)):
                    dis_mat[i][j]=float(cells[j])
                i+=1
        return dis_mat

    def __init__(self) -> None:
        self.customer,self.cus_cnt=self.read_points()
        self.dis=self.read_dis(self.cus_cnt)

    def part_of_cus(self,cus_ids):
        part_cus=[ self.customer[i] for i in cus_ids]
        cus_cnt=len(cus_ids)
        part_dis=np.zeros([cus_cnt,cus_cnt])
        for i in range(cus_cnt):
            for j in range(cus_cnt):
                _from= cus_ids[i]
                _to = cus_ids[j]
                part_dis[i][j]=self.dis[_from][_to]
        return part_cus,part_dis