#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
from random import random

import asyncio
import aiohttp
from aiohttp import web


class MultiPaxosHandler:
    PREPARE = 'prepare'
    ACCEPT = 'accept'
    LEARN = 'learn'
    HEARTBEAT = 'heartbeat'
    NO_OP = 'NOP'

    def __init__(self, ind, conf):
        self.nodes = conf['nodes']
        self.node_cnt = len(self.nodes)
        self.me = self.nodes[ind]
        self.ind = ind

        # leader election 
        self.last_heartbeat = 0
        self.heartbeat_ttl = conf['heartbeat']['ttl']
        self.heartbeat_interval = conf['heartbeat']['interval']
        self.election_slice = conf['election_slice']

        # leader
        self.n = (0, 0)
        self.seq = 0
        self.is_leader = False

        # acceptor
        self.n_promise = (0, 0)
        self.leader = None

        # states
        self.accepted = {}
        self.learned = [] # Bubble is represented by None
        
        # -
        self.loss_rate = conf['loss%'] / 100

        # -
        self.session = aiohttp.ClientSession(timeout=conf['misc']['request_timeout'])

    def make_url(node, command):
        return "http://{}:{}/{}".format(node['ip'], node['port'], method)

    async def make_requests(nodes, command, jsobj):
        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self.loss_rate:
                try:
                    async with session.post(make_url(node, command), json=jsobj) as resp:
                        resp_list.append((i, resp))
                except:
                    #resp_list.append((i, e))
                    pass
        return resp_list 

    async def elect(self, view_id):
        '''
        Compete for election.

        1. Setup up `n`
        2. Call prepare(n, bubble_slots, next_new_slot) to all nodes
        3. Count vote
        4. New leader if possible
            1. Initial heartbeat
            2. Accept!
        '''
        self.seq += 1
        self.n = (view_id, self.seq)

        # Only ask for things this proposer doesn't know.
        bubble_slots = []
        for i, v in enumerate(self.learned):
            if v == None:
                bubble_slots.append(i)

        jsobj = {
                'leader': self.ind,
                'n': self.n,
                'bubble_slots': bubble_slots,
                'next_new_slot': len(self.learned),
                }

        resp_list = await make_requests(self.nodes, MultiPaxosHandler.PREPARE, jsobj)

        # Count vote
        count = 0
        nv_for_known_slots = {}
        for i, resp in resp_list:
            if resp.status == 499: #TODO, give up
                return
            if resp.status == 200:
                data = await resp.json()
                for s, (n, v) in data['known_slots']:
                    if s in nv_for_known_slots:
                        nv_for_known_slots[s] = max(nv_for_known_slots[s], (n,v), key = lambda x: x[0])
                    else:
                        nv_for_known_slots[s] = (n, v)
                count += 1

        # New leader now
        if count >= n // 2 + 1:
            self.is_leader = True

            # Catch up
            for s in nv_for_known_slots:
                nv_for_known_slots[s] = (self.n, nv_for_known_slots[s][1])

            for b in bubble_slots:
                if not b in nv_for_known_slots:
                    nv_for_known_slots[b] = (self.n, MultiPaxosHandler.NO_OP)

            if nv_for_known_slots:
                max_slot_in_nv = max(nv_for_known_slots.keys())
            else:
                max_slot_in_nv = -1
            next_new_slot = max(max_slot_in_nv + 1, len(self.learned))
            for s in range(len(self.learned), next_new_slot):
                if s not in nv_for_known_slots:
                    nv_for_known_slots[s] = (self.n, MultiPaxosHandler.NO_OP)

            # First command: Accept!
            resp_list = make_requests(self.nodes, MultiPaxosHandler.ACCEPT, {'proposal': nv_for_known_slots})
            for i, resp in resp_list:
                if resp.status == 499:
                    # step down
                    self.is_leader = False
                    return

            # Initiate heartbeat!
            asyncio.ensure_future(self.heartbeat_server())

    async def heartbeat_server(self):
        '''
        Send heartbeat to every node.

        If get 499 as Nack, step down.
        '''
        while True:
            resp_list = make_requests(self.nodes, MultiPaxosHandler.HEARTBEAT, { 'leader': self.ind, 'n': self.n })
            for i, resp in resp_list:
                if resp.status == 499:
                    # step down
                    self.is_leader = False
                    return
            await asyncio.sleep(self.heartbeat_interval)

    async def heartbeat_observer(self):
        '''
        Observe hearbeat: if timeout, run election if appropriate.
        '''
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            cur_ts = time.time()
            if self.last_heartbeat < cur_ts - self.heartbeat_ttl:
                view_id = cur_ts / self.election_slice
                if view_id % self.node_cnt == self.ind:
                    # run election
                    asyncio.ensure_future(self.elect(view_id))

    async def heartbeat(self, request):
        '''
        Hearbeat sent from leader.
        {
            leader: 0,
            n: [ view_id, seq ],
        }

        respond with 200 if `n` is geq than current believed `n_promise`,
        else return 499
        '''

        jsobj = await request.json()
        n_hb = tuple(jsobj['n'])
        if n_hb > self.n_promise: #TODO: should be safe to update self.n_promise
            self.n_promise = n_hb
            self.leader = jsobj['leader']
            self.last_heartbeat = time.time()
            code = 200
        elif n_hb = self.n_promise:
            self.last_heartbeat = time.time()
            code = 200
        else:
            code = 499

        return web.Response(status=code)

    async def prepare(self, request):
        '''
        Prepare request from a proposer.

        {
            leader:
            n: 
            bubble_slots: []
            next_new_slot: 
        }

        respond with 200 as Ack if `n` is geq than current believed `n_promise`,
        else return 499 as Nack
        

        Ack:

        {
            known_slots: {
                0: (n, v),
                3: (n, v)
            }
        }
        '''
        jsobj = await request.json()
        n_prepare = tuple(jsobj['n'])
        if n_prepare > self.n_promise:
            self.n_promise = n_prepare
            self.leader = jsobj['leader']
            self.last_heartbeat = time.time() # also treat as a heartbeat

            bubble_slots = jsobj['bubble_slots']
            next_new_slot = jsobj['next_new_slot']

            known_slots = {}
            for b in bubble_slots:
                if b in self.accept:
                    known_slots[b] = self.accept[b]
                elif b < len(self.learned) and self.learned[b]: # return learned value directly, fake `n`
                    known_slots[b] = (n_prepare, self.learned[b])

            for b in self.accept:
                if b >= next_new_slot:
                    known_slots[b] = self.accept[b]
            
            for i in range(next_new_slot, len(self.learned)):
                if self.learned[i]:
                    known_slots[i] = (n_prepare, self.learned[i])
            
            jsobj = {'known_slots': known_slots}
            ret = web.json_response(jsobj)
        else:
            ret = web.Response(status=499)

        return ret

    async def accept(self, request):

        pass

    async def learn(self, request):
        pass

    async def request(self, request):
        pass

def logging_config(log_level=logging.INFO, log_file=None):
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return

    root_logger.setLevel(log_level)

    f = logging.Formatter("[%(levelname)s]%(module)s->%(funcName)s: \t %(message)s \t --- %(asctime)s")

    h = logging.StreamHandler()
    h.setFormatter(f)
    h.setLevel(log_level)
    root_logger.addHandler(h)

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        h = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        h.setFormatter(f)
        h.setLevel(log_level)
        root_logger.addHandler(h)

def arg_parse():
    # parse command line options
    parser = argparse.ArgumentParser(description='Multi-Paxos Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='/etc/paxos.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    args = parser.parse_args()
    return args


def conf_parse(conf_file) -> dict:
    '''
    Sample config:

    nodes:
        - ip: 
          port:
        - ip:
          port:

    loss%:

    skip:

    heartbeat:
        ttl:
        interval:

    election_slice: 10

    misc:
        request_timeout: 10
    '''
    conf = yaml.load(conf_file)
    return conf


def main():
    logging_config()
    log = logging.getLogger()
    args = arg_parse()
    conf = conf_parse(args.config)


if __name__ == "__main__":
    main()

