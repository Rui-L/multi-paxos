#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
from random import random
from collections import Counter

import asyncio
import aiohttp
from aiohttp import web


class MultiPaxosHandler:
    PREPARE = 'prepare'
    ACCEPT = 'accept'
    LEARN = 'learn'
    HEARTBEAT = 'heartbeat'
    REQUEST = 'request'
    SYNC = 'sync'

    NO_OP = 'NOP'

    def __init__(self, ind, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._me = self._nodes[ind]
        self._ind = ind

        # leader election
        self._last_heartbeat = 0
        self._heartbeat_ttl = conf['heartbeat']['ttl']
        self._heartbeat_interval = conf['heartbeat']['interval']
        self._election_slice = conf['election_slice']

        # leader
        self._n = (0, 0)
        self._seq = 0
        self._next_new_slot = 0
        self._is_leader = False
        self._hb_server = None

        # acceptor
        self._leader = None
        ## better named n_promise_accept, the largest number either promised or accepted
        self._n_promise = (0, 0)
        self._accepted = {} # s: (n, v)

        # learner
        self._sync_interval = conf['sync_interval']
        self._learning = {} # s: Counter((n, v): cnt)
        self._learned = [] # Bubble is represented by None
        self._learned_event = {} # s: event
 
        # -
        self._loss_rate = conf['loss%'] / 100

        # -
        self._network_timeout = conf['misc']['request_timeout']
        self._session = aiohttp.ClientSession(timeout=network_timeout)

    @staticmethod
    def make_url(node, command):
        return "http://{}:{}/{}".format(node['ip'], node['port'], method)

    async def make_requests(self, nodes, command, json_data):
        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                try:
                    async with session.post(self._make_url(node, command), json=json_data) as resp:
                        resp_list.append((i, resp))
                except:
                    #resp_list.append((i, e))
                    pass
        return resp_list 

    async def make_response(self, resp):
        '''
        Drop response by chance, via sleep for sometime.
        '''
        if random() < self._loss_rate:
            await asyncio.sleep(self._network_timeout)
        return resp

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
        self._seq += 1
        self._n = (view_id, self._seq)

        # Only ask for things this proposer doesn't know.
        bubble_slots = []
        for i, v in enumerate(self._learned):
            if v == None:
                bubble_slots.append(i)

        json_data = {
                'leader': self._ind,
                'n': self._n,
                'bubble_slots': bubble_slots,
                'next_new_slot': len(self._learned),
                }

        resp_list = await make_requests(self._nodes, MultiPaxosHandler.PREPARE, json_data)

        # Count vote
        count = 0
        nv_for_known_slots = {}
        for i, resp in resp_list:
            if resp.status == 499: #TODO, give up
                return
            if resp.status == 200:
                json_data = await resp.json()
                for s, (n, v) in json_data['known_slots']:
                    if s in nv_for_known_slots:
                        nv_for_known_slots[s] = max(nv_for_known_slots[s], (n,v), key = lambda x: x[0])
                    else:
                        nv_for_known_slots[s] = (n, v)
                count += 1

        # New leader now
        if count >= self._node_cnt // 2 + 1:
            self._is_leader = True

            # Catch up
            proposal = {k: v[1] for k, v in nv_for_known_slots.items()}

            for b in bubble_slots: # fill in bubble
                if not b in proposal:
                    proposal[b] = MultiPaxosHandler.NO_OP

            if proposal:
                max_slot_in_proposal = max(proposal.keys())
            else:
                max_slot_in_proposal = -1
            self._next_new_slot = max(max_slot_in_proposal + 1, len(self._learned))
            for s in range(len(self._learned), self._next_new_slot):
                if s not in proposal:
                    proposal[s] = MultiPaxosHandler.NO_OP

            # First command: Accept!
            json_data = {
                    'leader': self._ind,
                    'n': self._n,
                    'proposal': proposal
                    }

            resp_list = await make_requests(self._nodes, MultiPaxosHandler.ACCEPT, json_data)
            for i, resp in resp_list:
                if resp.status == 499:
                    # step down
                    self._is_leader = False
                    return

            # Initiate heartbeat!
            self._hb_server = asyncio.ensure_future(self._heartbeat_server())

    async def heartbeat_server(self._:
        '''
        Send heartbeat to every node.

        If get 499 as Nack, step down.
        '''
        while True:
            resp_list = await make_requests(self._nodes, MultiPaxosHandler.HEARTBEAT, { 'leader': self._ind, 'n': self._n })
            for i, resp in resp_list:
                if resp.status == 499:
                    # step down
                    self._is_leader = False
                    self._hb_server = None
                    return
            await asyncio.sleep(self._heartbeat_interval)

    async def heartbeat_observer(self._:
        '''
        Observe hearbeat: if timeout, run election if appropriate.
        '''
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            cur_ts = time.time()
            if self._last_heartbeat < cur_ts - self._heartbeat_ttl:
                view_id = cur_ts / self._election_slice
                if view_id % self._node_cnt == self._ind:
                    # run election
                    asyncio.ensure_future(self._elect(view_id))

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

        json_data = await request.json()
        n_hb = tuple(json_data['n'])
        if n_hb > self._n_promise: #TODO: should be safe to update self._n_promise
            self._n_promise = n_hb
            self._leader = json_data['leader']
            self._last_heartbeat = time.time()
            code = 200
        elif n_hb = self._n_promise:
            self._last_heartbeat = time.time()
            code = 200
        else:
            code = 499

        return await self._make_response(web.Response(status=code))

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
        json_data = await request.json()
        n_prepare = tuple(json_data['n'])
        if n_prepare > self._n_promise: # Only Ack if n_prepare larger than the largest n ever seen.
            self._n_promise = n_prepare
            self._leader = json_data['leader']
            self._last_heartbeat = time.time() # also treat as a heartbeat

            bubble_slots = json_data['bubble_slots']
            next_new_slot = json_data['next_new_slot']

            known_slots = {}
            for b in bubble_slots:
                if b in self._accept:
                    known_slots[b] = self._accept[b]
                elif b < len(self._learned) and self._learned[b]: # return learned value directly, fake `n`
                    known_slots[b] = (n_prepare, self._learned[b])

            for b in self._accept:
                if b >= next_new_slot:
                    known_slots[b] = self._accept[b]

            for i in range(next_new_slot, len(self._learned)):
                if self._learned[i]:
                    known_slots[i] = (n_prepare, self._learned[i])

            json_data = {'known_slots': known_slots}
            ret = web.json_response(json_data)
        else:
            ret = web.Response(status=499)

        return await self._make_response(ret)

    async def accept(self, request):
        '''
        Accept request from a proposer.

        Accept: {
            leader:
            n: 
            proposal: {
                0: v,
                13: v,
            }
        }

        respond with 200 as OK if `n` is geq than current believed `n_promise`,
        else return 499 as Nack

        Make LEARN request to all nodes if accepted.
        '''
        json_data = await request.json()
        n_accept = tuple(json_data['n'])
        if n_accept >= self._n_promise: # Only Accept if n_accept geq than the largest n ever seen.
            self._n_promise = n_accept
            self._leader = json_data['leader']
            proposal = json_data['proposal']
            for s, v in proposal:
                if not s < len(self._learned) or not self._learned[s]:
                    self._accepted[s] = (n_accept, v)
                asyncio.ensure(make_requests(self._nodes, MultiPaxosHandler.LEARN, { 'proposal': proposal }))
            ret = web.Response()
        else:
            ret = web.Response(status=499)

        return await self._make_response(ret)

    async def learn(self, request):
        '''
        Learn request from nodes.

        {
            proposal: {
                0: v,
                3: v,
            }
        }

        No response needed
        '''

        json_data = await request.json()
        proposal = json_data['proposal']
        for s, v in proposal:
            if not s < len(self._learned) or not self._learned[s]:
                if not s in self._learning:
                    self._learning[s] = Counter()
                self._learning[s][(n,v)] += 1
                if self._learning[s][(n,v)] >= self._node_cnt // 2 + 1:
                    if not s < len(self._learned):
                        self._learned += [None] * (s + 1 - len(self._learned))
                    self._learned[s] = v
                    if s in self._learned_event:
                        self._learned_event[s].set()
                        del self._learned_event[s]
                    del self._learning[s]
                    if s in self._accepted:
                        del self._accepted[s]

        return await self._make_response(web.Response())

    async def decision_synchronizer(self._:
        '''
        Sync with other learner periodically.

        Input/output similar to `prepare`
    
        {
            known_slots: {
                0: v,
                3: v
            }
        }
        '''
        while True:
            await asyncio.sleep(self._sync_interval)
            if not self._is_leader:
                # Only ask for things this learner doesn't know.
                bubble_slots = []
                for i, v in enumerate(self._learned):
                    if v == None:
                        bubble_slots.append(i)

                json_data = {
                        'bubble_slots': bubble_slots,
                        'next_new_slot': len(self._learned),
                        }

                resp_list = await self._make_requests([self._nodes[self._leader]], MultiPaxosHandler.SYNC, json_data)
                if not resp_list:
                    resp = resp_list[0]
                    json_data = await resp.json()
                    for s, v in json_data['known_slots']:
                        if not s < len(self._learned):
                            self._learned += [None] * (s + 1 - len(self._learned))
                        self._learned[s] = v
                        if s in self._learned_event:
                            self._learned_event[s].set()
                            del self._learned_event[s]
                        if s in self._learning:
                            del self._learning[s]
                        if s in self._accepted:
                            del self._accepted[s]

    async def sync(self, request):
        '''
        Sync request from learner.

        {
            bubble_slots: []
            next_new_slot: 
        }
        '''
        json_data = await request.json()

        bubble_slots = json_data['bubble_slots']
        next_new_slot = json_data['next_new_slot']

        known_slots = {}
        for b in bubble_slots:
            if b < len(self._learned) and self._learned[b]:
                known_slots[b] = self._learned[b]
        for i in range(next_new_slot, len(self._learned)):
            if self._learned[i]:
                known_slots[i] = self._learned[i]

        json_data = {'known_slots': known_slots}
        
        return await make_response(web.json_response(json_data))

    async def request(self, request):
        '''
        Request from client.

        {
            id: (cid, seq),
            data: "string"
        }

        Handle this if leader, otherwise redirect to leader
        '''

        json_data = await request.json()
        if not self._is_leader:
            raise web.HTTPTemporaryRedirect(self._make_url(self._nodes[self._leader], MultiPaxosHandler.REQUEST))

        this_slot = self._next_new_slot
        self._next_new_slot += 1
        proposal = {
                'leader': self._ind,
                'n': self._n,
                'proposal': {
                    this_slot: (json_data['id'], json_data['data'])
                    }
                }
        if not this_slot in self._learned_event:
            self._learned_event[this_slot] = asyncio.Event()
        this_event = self._learned_event[this_slot]

        resp_list = await make_requests(self._nodes, MultiPaxosHandler.ACCEPT, proposal)
        for i, resp in resp_list:
            if resp.status == 499:
                # step down
                self._is_leader = False
                if self._hb_server:
                    self._hb_server.cancel()
                    self._hb_server = None
                raise web.HTTPTemporaryRedirect(self._make_url(self._nodes[self._leader], MultiPaxosHandler.REQUEST))

        # respond only when learner learned the value
        await this_event.wait()
        if self._learned[this_slot] == (json_data['id'], json_data['data']):
            resp = web.Response()
        else:
            raise web.HTTPTemporaryRedirect(self._make_url(self._nodes[self._leader], MultiPaxosHandler.REQUEST))

        return await self._make_response(resp)


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

    sync_interval: 10

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

    host, port = conf['nodes'][args.index]

    paxos = MultiPaxosHandler(args.index, conf)
    asyncio.ensure_future(paxos.heartbeat_observer())
    asyncio.ensure_future(paxos.decision_synchronizer())

    app = web.Application()
    app.add_routes([
        web.post('/' + MultiPaxosHandler.HEARTBEAT, paxos.heartbeat),
        web.post('/' + MultiPaxosHandler.PREPARE, paxos.prepare),
        web.post('/' + MultiPaxosHandler.ACCEPT, paxos.accept),
        web.post('/' + MultiPaxosHandler.LEARN, paxos.learn),
        web.post('/' + MultiPaxosHandler.SYNC, paxos.sync),
        web.post('/' + MultiPaxosHandler.REQUEST, paxos.request),
        ])

    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()

