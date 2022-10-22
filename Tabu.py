from dis import dis
from math import ceil
import numpy as np
from itertools import combinations
import random
# np.random.seed(111)
# random.seed(222)
rand_int = np.random.randint
rand_arr = np.random.choice


class Para:

    def __init__(self, **args) -> None:
        self.weight_UB = args['weight_UB']
        self.dis = args['dis']
        self.maxn_percent = args['maxn_percent']
        self.cus = args['cus']
        self.weight = args['weight']
        dis_sort = list()
        for i in range(len(self.dis)):
            dis_sort.append([(j, self.dis[i][j])
                             for j in range(len(self.dis))])
        for d in dis_sort:
            d.sort(key=lambda x: x[1])
        self.dis_sort = dis_sort


class Route:

    def __init__(self, para: Para, route: list = None):
        self.route = np.array(route)
        self.feasible = True
        self.para = para

    def __len__(self):
        return len(self.route)

    def __getitem__(self, item):
        return self.route[item]

    def __setitem__(self, item, val):
        self.route[item] = val

    def __str__(self) -> str:
        return self.route.__str__()

    def get_cost(self, alpha):
        weight_sum = 0
        for i in range(1, len(self.route)):
            weight_sum += self.para.weight[self.route[i]]
        weight_sum = 0 if weight_sum <= self.para.weight_UB else weight_sum - self.para.weight_UB
        dis_sum = 0
        for i in range(1, len(self.route)):
            dis_sum += self.para.dis[self.route[i - 1]][self.route[i]]
        self.cost = dis_sum + alpha * weight_sum
        return self.cost, weight_sum == 0

    def get_delta_d(self):
        delta_d = np.zeros(len(self.route) - 1)
        delta_d[0] = -np.inf
        pre_set = set([0])
        for i in range(1, len(self.route) - 1):
            delta_d[i] = self.para.dis[self.route[i - 1]][self.route[i]]
            for one_dis_sort in self.para.dis_sort[self.route[i]]:
                if one_dis_sort[0] in pre_set:
                    delta_d[i] -= one_dis_sort[1]
                    break
            pre_set.add(self.route[i])
        delta_d_asce = np.argsort(delta_d)
        return delta_d_asce[::-1]

    def copy(self):
        route = Route(self.para)
        route.route = self.route.copy()
        route.cost = self.cost
        route.feasible = self.feasible
        return route


class Routes:

    def __init__(self, routes: list, maxn_percent):
        self.routes = routes
        self.total_cost = 0
        self.maxn_percent = maxn_percent
        for route in routes:
            rc, _ = route.get_cost(0)
            self.total_cost += rc

    def copy(self):
        new_routes = Routes([], self.maxn_percent)
        routes_list = [route.copy() for route in self.routes]
        new_routes.routes = routes_list
        new_routes.total_cost = self.total_cost
        return new_routes

    def combine(self, other_routes):
        self.routes.extend(other_routes.routes)
        self.total_cost += other_routes.total_cost

    def __getitem__(self, item):
        return self.routes[item]

    def __str__(self) -> str:
        s = ""
        for route in self.routes:
            s += route.__str__()
            s += ';\n'
        return s

    def __len__(self):
        return len(self.routes)

    def __setitem__(self, item, val):
        self.routes[item] = val

    def feasible(self):
        for route in self.routes:
            if not route.feasible:
                return False
        return True

    def get_delta_d(self):
        delta_ds = [route.get_delta_d() for route in self.routes]
        return delta_ds

    def two_opt(self, alpha, desc_delta_d):
        num = rand_int(len(self.routes))
        while len(self.routes[num]) <= 3:
            num = rand_int(len(self.routes))
        now_route = self.routes[num].copy()
        total_cost = self.total_cost - now_route.cost
        max_N = ceil((len(now_route) - 2) * self.maxn_percent)
        if max_N < 2:
            max_N = 2
        random_tow = np.random.choice(desc_delta_d[num][:max_N], 2, False)
        now_route[random_tow[0]], now_route[random_tow[1]] = now_route[
            random_tow[1]], now_route[random_tow[0]]
        new_cost, now_route.feasible = now_route.get_cost(alpha)
        total_cost += new_cost
        return total_cost, [now_route], [num], [
            now_route[random_tow[0]], now_route[random_tow[1]]
        ]

    def swap(self, alpha, desc_delta_d):
        two_route = rand_arr(len(self.routes), 2, False)
        r1 = self.routes[two_route[0]].copy()
        r2 = self.routes[two_route[1]].copy()
        total_cost = self.total_cost - r1.cost - r2.cost
        max_N = ceil((len(r1) - 2) * self.maxn_percent)
        first = np.random.choice(desc_delta_d[two_route[0]][:max_N])
        second = rand_int(1, len(r2) - 1)
        r1[first], r2[second] = r2[second], r1[first]
        cost1, r1.feasible = r1.get_cost(alpha)
        cost2, r2.feasible = r2.get_cost(alpha)
        total_cost += cost1 + cost2
        return total_cost, (r1, r2), two_route, [r1[first], r2[second]]

    def crossover(self, alpha, desc_delta_d):
        two_route = rand_arr(len(self.routes), 2, False)
        r1 = self.routes[two_route[0]].copy()
        r2 = self.routes[two_route[1]].copy()
        total_cost = self.total_cost - r1.cost - r2.cost
        max_N = ceil((len(r1) - 2) * self.maxn_percent)
        mid1 = np.random.choice(desc_delta_d[two_route[0]][:max_N])
        mid2 = rand_int(1, len(r2) - 1)
        r1l = r1[0:mid1]
        r1r = r1[mid1:]
        r2l = r2[0:mid2]
        r2r = r2[mid2:]
        r1.route = np.append(r1l, r2r, 0)
        r2.route = np.append(r2l, r1r, 0)
        cost1, r1.feasible = r1.get_cost(alpha)
        cost2, r2.feasible = r2.get_cost(alpha)
        total_cost += cost1 + cost2
        return total_cost, (r1, r2), two_route, [r1[mid1], r2[mid2]]

    def reinsertion(self, alpha, desc_delta_d):
        two_route = rand_arr(len(self.routes), 2, True)
        while two_route[0] == two_route[1] and len(
                self.routes[two_route[0]]) <= 3:
            two_route = rand_arr(len(self.routes), 2, True)
        if two_route[0] == two_route[1]:
            r1 = self.routes[two_route[0]].copy()
            total_cost = self.total_cost - r1.cost
            max_N = ceil((len(r1) - 2) * self.maxn_percent)
            two_index = np.random.choice(desc_delta_d[two_route[0]][:max_N], 2,
                                         False)
            r1[two_index[0]], r1[two_index[1]] = r1[two_index[1]], r1[
                two_index[0]]
            cost1, r1.feasible = r1.get_cost(alpha)
            total_cost += cost1
            return total_cost, [r1], [two_route[0]
                                      ], [r1[two_index[0]], r1[two_index[1]]]
        else:
            r1 = self.routes[two_route[0]].copy()
            r2 = self.routes[two_route[1]].copy()
            total_cost = self.total_cost - r1.cost - r2.cost
            max_N = ceil((len(r1) - 2) * self.maxn_percent)
            mid_index = np.random.choice(desc_delta_d[two_route[0]][:max_N])
            mid = r1[mid_index]
            r1.route = np.append(r1[0:mid_index], r1[mid_index + 1:])
            mid_index2 = rand_int(1, len(r2) - 1)
            r2.route = np.append(r2[0:mid_index2],
                                 np.append(mid, r2[mid_index2:]))
            cost1, r1.feasible = r1.get_cost(alpha)
            cost2, r2.feasible = r2.get_cost(alpha)
            total_cost += cost1 + cost2
            return total_cost, (r1, r2), two_route, [mid, r2[mid_index2]]


class Tabu:

    def __init__(self, cus_list, tabu_len, disturb_iter, parent_id, self_id,
                 para) -> None:
        cus_cnt = len(cus_list)
        self.cus_cnt = cus_cnt
        self.cus_list = cus_list
        total_cus_len = len(para.cus) - 1
        self.two_opt_tl = np.zeros([total_cus_len, total_cus_len])
        self.swap_tl = np.zeros([total_cus_len, total_cus_len])
        self.crossover_tl = np.zeros([total_cus_len, total_cus_len])
        self.reinsertion_tl = np.zeros([total_cus_len, total_cus_len])
        self.tabu_len = tabu_len
        self.operator_and_tl = [(Routes.two_opt, self.two_opt_tl),
                                (Routes.swap, self.swap_tl),
                                (Routes.crossover, self.crossover_tl),
                                (Routes.reinsertion, self.reinsertion_tl)]
        self.disturb_iter = disturb_iter
        self.parent_id = parent_id
        self.part_index = 0
        self.id = self_id
        self.combine_ids = list()
        self.para = para
        self.best_routes = []
        self.best_cost = None

    def init_solution(self):
        routes = list()
        rest_cus = set(self.cus_list)
        while len(rest_cus) > 0:
            route = [0]
            find = False
            for i in range(len(self.para.dis_sort[0])):
                if self.para.dis_sort[0][i][0] in rest_cus:
                    cus_id = self.para.dis_sort[0][i][0]
                    route.append(cus_id)
                    rest_cus.remove(cus_id)
                    find = True
                    break
            if not find:
                break
            rest_wegiht = self.para.weight_UB - self.para.cus[cus_id][3]
            while True:
                find = False
                for nxt in self.para.dis_sort[cus_id]:
                    if nxt[0] != cus_id and nxt[0] != 0 and nxt[
                            0] in rest_cus and rest_wegiht >= self.para.cus[
                                nxt[0]][3]:
                        rest_wegiht -= self.para.cus[nxt[0]][3]
                        cus_id = nxt[0]
                        rest_cus.remove(cus_id)
                        route.append(cus_id)
                        find = True
                        break
                if not find:
                    route.append(0)
                    routes.append(Route(self.para, route))
                    break
        routes = Routes(routes, self.para.maxn_percent)
        self.best_cost = routes.total_cost
        return routes

    def __repr__(self) -> str:
        # string=str(self.cus_list)
        string = ''
        for route in self.best_routes:
            string += str(route) + '\n'
        string += "*" * 10 +'*'*10
        string += 'best cost {0}'.format(self.best_cost)
        return string

    def disturb(self, routes, alpha):
        def two_opt(comb, route):
            route[comb[0]], route[comb[1]] = route[comb[1]], route[comb[0]]
            cost, route.feasible = route.get_cost(alpha)
            return cost, route
        new_routes = list()
        for route in routes:
            if len(route) <= 3:
                new_routes.append(route)
                continue
            combs = combinations(range(1, len(route) - 1), 2)
            record = list()
            for comb in combs:
                record.append(two_opt(comb, route.copy()))
            record.sort(key=lambda x: x[0])
            prob = [1 / (2**i) for i in range(1, len(record) + 1)]
            new_route = random.choices(record, prob)[0]
            new_routes.append(new_route[1])
        return Routes(new_routes, self.best_routes.maxn_percent)

    def one_iter(self, routes: Routes, alpha, max_i):

        def step(tl):
            for x in np.nditer(tl, op_flags=['readwrite']):
                x[...] = x - 1 if x > 0 else 0

        def step_all():
            step(self.swap_tl)
            step(self.crossover_tl)
            step(self.reinsertion_tl)
            step(self.two_opt_tl)

        step_all()
        if len(routes) == 1:
            func, tl = Routes.two_opt, self.two_opt_tl
        else:
            func, tl = random.choice(self.operator_and_tl)
        desc_delta_d = routes.get_delta_d()

        i = 0
        routes0 = routes.copy()
        temp_best_cost = np.inf
        no_get_in = 0
        while i < max_i:
            routes = routes0.copy()
            new_total_cost, new_routes, new_route_ids, change_cus = func(
                routes, alpha, desc_delta_d)
            if (tl[change_cus[0]-1][change_cus[1]-1]<= 0 and tl[change_cus[1]-1][change_cus[0]-1] <=0) or \
                new_total_cost<self.best_cost:
                i += 1
                new_feasible = True
                for id, new_route in zip(new_route_ids, new_routes):                    
                    routes[id] = new_route
                    new_feasible = new_feasible and new_route.feasible
                for new_route in new_routes:
                    if len(new_route) <= 2:
                        routes.routes.remove(new_route)
                routes.total_cost = new_total_cost
                tl[change_cus[0] - 1][change_cus[1] - 1] = tl[change_cus[1] - 1][change_cus[0] - 1] = self.tabu_len
                if new_total_cost < self.best_cost:
                    self.best_routes = routes.copy()
                    self.best_cost = new_total_cost
                if new_total_cost < temp_best_cost:
                    temp_best = routes
                    temp_best_cost = new_total_cost
                no_get_in = 0

            else:
                no_get_in += 1
                if no_get_in > 2:
                    step_all()
                    no_get_in = 0
        if alpha > 1000:
            alpha = 1000
        if temp_best.feasible():
            alpha /= 1.5
        else:
            alpha *= 1.5
        return temp_best, alpha

    def search(self, routes, max_iter: int):
        self.best_routes = routes
        self.best_cost = routes.total_cost
        if self.cus_cnt == 1:
            return
        alpha = 10
        iter = 0
        max_i = min(ceil(max_iter / 10), 10)
        while iter < max_iter:
            iter += 1
            routes, alpha = self.one_iter(routes, alpha, max_i)
            if iter % self.disturb_iter == 0:
                routes = self.disturb(self.best_routes, alpha)
          

    def copy(self):
        cpy = Tabu(self.cus_list, self.tabu_len, self.disturb_iter,
                   self.parent_id, self.id, self.para)
        cpy.best_routes = self.best_routes
        cpy.best_cost = self.best_cost
        return cpy

    def combine(self, other_tabu):
        self.cus_cnt = self.cus_cnt + other_tabu.cus_cnt
        self.cus_list.extend(other_tabu.cus_list)
        self.tabu_len = ceil(self.cus_cnt / 10)
        self.best_routes.combine(other_tabu.best_routes)
        self.best_cost += other_tabu.best_cost

