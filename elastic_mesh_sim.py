#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import sys, os
import random
import json

# default cmdline args
cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument('--config', type=str, help='JSON config file to control the simulations', required=True)

class Logger(object):
    def __init__(self, env, filename):
        self.env = env
        self.filename = filename
        self.prefix = ''

    @staticmethod
    def init_params():
        Logger.debug = MeshSimulator.config['debug'].next()

    def log(self, s):
        if Logger.debug:
            data = '{}: {}'.format(self.env.now, s)
            print data
            with open(self.filename, 'a') as f:
                f.write(data + '\n')


class Message(object):
    """Message sent between nodes
    """
    count = 0
    def __init__(self, src, dst, iteration):
        self.src = src
        self.dst = dst
        self.iteration = iteration
        self.ID = Message.count
        Message.count += 1

    @staticmethod
    def init_params():
        pass

    def __str__(self):
        return "Message {}: src={}, dst={}, iteration={}".format(self.ID, self.src, self.dst, self.iteration)

class Start(Message):
    def __str__(self):
        return super(Start, self).__str__() + " (Start)"

class Request(Message):
    def __str__(self):
        return super(Request, self).__str__() + " (Request)"

class Response(Message):
    def __str__(self):
        return super(Response, self).__str__() + " (Response)"

class Node(object):
    """Each Node is a single nanoservice. A Node represents a single body in the mesh."""
    count = 0
    def __init__(self, env, logger, network):
        self.env = env
        self.logger = logger
        self.network = network
        self.queue = simpy.FilterStore(self.env)
        self.ID = Node.count
        Node.count += 1
        self.iteration_cnt = 0 # local iteration counter
        self.request_cnt = 0 # count requests processed in each iteration
        self.response_cnt = 0 # count responses in each iteration
        self.neighbors = []
        self.env.process(self.start())

    @staticmethod
    def init_params():
        Node.request_service_time = Node.sample_generator(MeshSimulator.config['request_service_time_dist'].next())
        Node.response_service_time = Node.sample_generator(MeshSimulator.config['response_service_time_dist'].next())
        Node.enable_incast_prevention = MeshSimulator.config['enable_incast_prevention'].next()

    @staticmethod
    def sample_generator(filename):
        # read the file and generate samples
        samples = pd.read_csv(filename)
        while True:
            yield random.choice(samples['all'])

    def add_neighbor(self, ID):
        self.neighbors.append(ID)

    def log_prefix(self):
        return 'Node {}: iteration {}: request_cnt: {} response_cnt: {} '.format(self.ID, self.iteration_cnt, self.request_cnt, self.response_cnt)

    def start(self):
        """Receive and process messages"""
        while not MeshSimulator.complete:
            # wait for a msg that is for the current iteration
            self.logger.log(self.log_prefix() + 'Waiting for message to arrive ...')
            # make sure that we process all requests in the iteration before processing our responses
            msg = yield self.queue.get(filter = lambda msg: (msg.iteration == self.iteration_cnt) and (type(msg) != Response or (type(msg) == Response and self.request_cnt == len(self.neighbors))))
            self.logger.log(self.log_prefix() + 'Processing message: {}'.format(str(msg)))
            if type(msg) == Request:
                self.request_cnt += 1
                # process request and send back response
                service_time = Node.request_service_time.next()
                yield self.env.timeout(service_time)
                # TODO: prehaps perform incast prevention here as well? Don't think it is needed because requests shouldn't be synchronized here
                self.network.queue.put(Response(self.ID, msg.src, msg.iteration))
            elif type(msg) == Response:
                self.response_cnt += 1
                self.logger.log(self.log_prefix() + 'Received {} responses out of {}'.format(self.response_cnt, len(self.neighbors)))
                if self.response_cnt == len(self.neighbors):
                    # all responses received
                    service_time = Node.response_service_time.next()
                    yield self.env.timeout(service_time)
                    # reset for the next iteration
                    self.iteration_cnt += 1
                    self.request_cnt = 0
                    self.response_cnt = 0
                    # check if the simulation has completed
                    MeshSimulator.check_done(self.env.now)
                    self.logger.log(self.log_prefix() + 'MeshSimulator.complete={}, MeshSimulator.num_iterations={}'.format(MeshSimulator.complete, MeshSimulator.num_iterations))
                    #if self.iteration_cnt < MeshSimulator.num_iterations:
                    if not MeshSimulator.complete:
                        self.logger.log(self.log_prefix() + 'Sending requests to neighbors: {}'.format(str(self.neighbors)))
                        for n in self.neighbors:
                            msg = Request(self.ID, n, self.iteration_cnt)
                            # optionally perform incast prevention
                            if Node.enable_incast_prevention == 1:
                                # wait our turn to send
                                delay = Node.request_service_time.next() * random.randint(0,3)
                                self.logger.log(self.log_prefix() + 'incast prevention enabled, will delay sending for {} ns'.format(delay))
                                self.env.process(self.transmit_msg(msg, delay))
                            else:
                                # send immediately
                                self.network.queue.put(msg)
            elif type(msg) == Start:
                # send out initial requests
                self.logger.log(self.log_prefix() + 'Sending requests to neighbors: {}'.format(str(self.neighbors)))
                for n in self.neighbors:
                    self.network.queue.put(Request(self.ID, n, self.iteration_cnt))

    def transmit_msg(self, msg, delay):
        self.logger.log(self.log_prefix() + 'Starting to transmit msg: {}'.format(str(msg)))
        # model the network communication delay
        yield self.env.timeout(delay)
        # put the message in the node's queue
        self.network.queue.put(msg)
        self.logger.log(self.log_prefix() + 'Transmitted msg: {}'.format(str(msg)))

class Network(object):
    """Network which moves messages between nodes"""
    def __init__(self, env, logger):
        self.env = env
        self.logger = logger
        self.queue = simpy.Store(env)
        self.nodes = []
        self.env.process(self.start())

    @staticmethod
    def init_params():
        Network.delay = MeshSimulator.config['network_delay'].next()

    def add_nodes(self, nodes):
        self.nodes += nodes

    def start(self):
        """Start forwarding messages"""
        while not MeshSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Switching msg\n\t"{}"'.format(str(msg)))
            # need to kick this off asynchronously so this is a non-blocking network
            self.env.process(self.transmit_msg(msg))

    def transmit_msg(self, msg):
        # model the network communication delay
        yield self.env.timeout(Network.delay)
        # put the message in the node's queue
        self.nodes[msg.dst].queue.put(msg)


def DistGenerator(dist, **kwargs):
    if dist == 'bimodal':
        bimodal_samples = map(int, list(np.random.normal(kwargs['lower_mean'], kwargs['lower_stddev'], kwargs['lower_samples']))
                                   + list(np.random.normal(kwargs['upper_mean'], kwargs['upper_stddev'], kwargs['upper_samples'])))
    while True:
        if dist == 'uniform':
            yield random.randint(kwargs['min'], kwargs['max'])
        elif dist == 'normal':
            yield int(np.random.normal(kwargs['mean'], kwargs['stddev']))
        elif dist == 'poisson':
            yield np.random.poisson(kwargs['lambda']) 
        elif dist == 'lognormal':
            yield int(np.random.lognormal(kwargs['mean'], kwargs['sigma']))
        elif dist == 'exponential':
            yield int(np.random.exponential(kwargs['lambda']))
        elif dist == 'fixed':
            yield kwargs['value']
        elif dist == 'bimodal':
            yield random.choice(bimodal_samples)
        else:
            print 'ERROR: Unsupported distrbution: {}'.format(dist)
            sys.exit(1)


class MeshSimulator(object):
    """This class controls the simulation"""
    config = {} # user specified input
    out_dir = 'out'
    out_run_dir = 'out/run-0'
    # run local variables
    complete = False
    converged_node_cnt = 0
    iteration_cnt = 0
    finish_time = 0
    # global logs (across runs)
    avg_throughput = {'all':[]} # iterations/ns
    def __init__(self, env):
        self.env = env
        MeshSimulator.num_iterations = MeshSimulator.config['num_iterations'].next()
        MeshSimulator.grid_size = MeshSimulator.config['grid_size'].next()
        MeshSimulator.sample_period = MeshSimulator.config['sample_period'].next()
        network_log_file = os.path.join(MeshSimulator.out_run_dir, 'network.log')
        # clear network log file
        with open(network_log_file, 'w') as f:
            f.write('')
        self.network = Network(self.env, Logger(env, network_log_file))

        Message.count = 0
        Node.count = 0

        # create nodes
        self.nodes = []
        for i in range(MeshSimulator.grid_size**2):
            node_log_file = os.path.join(MeshSimulator.out_run_dir, 'node-{}.log'.format(i))
            # clear node log file
            with open(node_log_file, 'w') as f:
                f.write('')
            self.nodes.append(Node(self.env, Logger(env, node_log_file), self.network))

        # add neighbors to each node
        for i in range(MeshSimulator.grid_size**2):
            n = MeshSimulator.grid_size
            # north neighbor
            if (i-n) >= 0:
                self.nodes[i].add_neighbor(i-n)
            # south neighbor
            if (i+n) < n**2:
                self.nodes[i].add_neighbor(i+n)
            # east neighbor
            if (i+1) % n != 0:
                self.nodes[i].add_neighbor(i+1)
            # west neighbor
            if i % n != 0: 
                self.nodes[i].add_neighbor(i-1)

        # connect nodes to network
        self.network.add_nodes(self.nodes)
        
        self.init_sim()

    def init_sim(self):
        # initialize run local variables
        self.q_sizes = {n.ID:[] for n in self.nodes}
        self.q_sizes['time'] = []
        self.q_sizes['network'] = []
        MeshSimulator.complete = False
        MeshSimulator.converged_node_cnt = 0
        MeshSimulator.iteration_cnt = 0
        MeshSimulator.finish_time = 0
        # send out Start messages
        for n in self.nodes:
            n.queue.put(Start(0, n.ID, 0))
        # start logging
        if self.sample_period > 0:
            self.env.process(self.sample_queues())

    def sample_queues(self):
        """Sample node queue occupancy"""
        while not MeshSimulator.complete:
            self.q_sizes['time'].append(self.env.now)
            self.q_sizes['network'].append(len(self.network.queue.items))
            for n in self.nodes:
                self.q_sizes[n.ID].append(len(n.queue.items))
            yield self.env.timeout(MeshSimulator.sample_period)

    @staticmethod
    def check_done(now):
        """This is called by each node after receiving all responses in each iteration"""
        MeshSimulator.converged_node_cnt += 1
        # increment the iteration_cnt if all nodes have converged
        if MeshSimulator.converged_node_cnt == MeshSimulator.grid_size**2:
            MeshSimulator.iteration_cnt += 1
            MeshSimulator.converged_node_cnt = 0
        # simulation is complete after all iterations
        if MeshSimulator.iteration_cnt == MeshSimulator.num_iterations:
            MeshSimulator.complete = True
            MeshSimulator.finish_time = now

    def dump_run_logs(self):
        """Dump any logs recorded during this run of the simulation"""
        out_dir = os.path.join(os.getcwd(), MeshSimulator.out_run_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # log the measured avg queue sizes
        df = pd.DataFrame(self.q_sizes)
        write_csv(df, os.path.join(MeshSimulator.out_run_dir, 'q_sizes.csv'))

        # record avg throughput for this run in terms of iterations/microsecond
        throughput = 1e3*float(MeshSimulator.num_iterations)/(MeshSimulator.finish_time)
        MeshSimulator.avg_throughput['all'].append(throughput)

    @staticmethod
    def dump_global_logs():
        # log avg throughput
        df = pd.DataFrame(MeshSimulator.avg_throughput)
        write_csv(df, os.path.join(MeshSimulator.out_dir, 'avg_throughput.csv'))

def write_csv(df, filename):
    with open(filename, 'w') as f:
            f.write(df.to_csv(index=False))

def param(x):
    while True:
        yield x

def param_list(L):
    for x in L:
        yield x

def parse_config(config_file):
    """ Convert each parameter in the JSON config file into a generator
    """
    with open(config_file) as f:
        config = json.load(f)

    for p, val in config.iteritems():
        if type(val) == list:
            config[p] = param_list(val)
        else:
            config[p] = param(val)

    return config

def run_mesh_sim(cmdline_args):
    MeshSimulator.config = parse_config(cmdline_args.config)
    # make sure output directory exists
    MeshSimulator.out_dir = MeshSimulator.config['out_dir'].next()
    out_dir = os.path.join(os.getcwd(), MeshSimulator.out_dir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # copy config file into output directory
    os.system('cp {} {}'.format(cmdline_args.config, out_dir))
    # run the simulations
    run_cnt = 0
    try:
        while True:
            print 'Running simulation {} ...'.format(run_cnt)
            # initialize random seed
            random.seed(1)
            np.random.seed(1)
            # init params for this run on all classes
            Logger.init_params()
            Message.init_params()
            Node.init_params()
            Network.init_params()
            MeshSimulator.out_run_dir = os.path.join(MeshSimulator.out_dir, 'run-{}'.format(run_cnt))
            run_cnt += 1
            env = simpy.Environment()
            s = MeshSimulator(env)
            env.run()
            s.dump_run_logs()
    except StopIteration:
        MeshSimulator.dump_global_logs()
        print 'All Simulations Complete!'

def main():
    args = cmd_parser.parse_args()
    # Run the simulation
    run_mesh_sim(args)

if __name__ == '__main__':
    main()

