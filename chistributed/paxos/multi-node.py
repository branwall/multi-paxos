from Queue import Queue
import random
import json
import sys
import signal
import time
import zmq
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

# Superclass for encapsulating ZeroMQ code
class Node:
  MAX_MISSED_HEARTBEATS = 3

  def __init__(self, node_name, pub_endpoint, router_endpoint, 
               peer_names, handle_func, proposers, acceptors):
    self.loop = ioloop.ZMQIOLoop.current()
    self.context = zmq.Context()

    self.connected = False

    # SUB socket for receiving messages from the broker
    self.sub_sock = self.context.socket(zmq.SUB)
    self.sub_sock.connect(pub_endpoint)
    # make sure we get messages meant for us!
    self.sub_sock.set(zmq.SUBSCRIBE, node_name)
    self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
    self.sub.on_recv(handle_func)

    # REQ socket for sending messages to the broker
    self.req_sock = self.context.socket(zmq.REQ)
    self.req_sock.connect(router_endpoint)
    self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
    self.req.on_recv(self.handle_broker_message)

    self.name = node_name
    self.peer_names = peer_names
    self.acceptors = acceptors if acceptors else self.peer_names
    self.other_proposers = list(set(proposers).difference({self.name}))

    self.store = {}

    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
      signal.signal(sig, self.shutdown)

  def handle_broker_message(self, msg_frames):
    '''
    Nothing important to do here yet.
    '''
    pass

  def paxos_message(self, type, num, dst, value=None):
    message = {'type': 'paxos', 'msg': type, 'num': num, 'src': self.name,
               'destination': dst}
    if value:
      message['value'] = value
    return message

  def respond_to_hello(self):
    if not self.connected:
      self.connected = True
      self.req.send_json({'type': 'helloResponse', 'source': self.name})

  def log(self, debug_info):
    self.req.send_json({'type': 'log', 'debug': debug_info})

  def start(self):
    '''
    Simple manual poller, dispatching received messages and sending those in
    the message queue whenever possible.
    '''
    self.loop.start()

  def shutdown(self, sig, frame):
    self.loop.stop()
    self.sub_sock.close()
    self.req_sock.close()
    sys.exit(0)

class Acceptor(Node):
  def __init__(self, *zmq_args):
    zmq_args = tuple(list(zmq_args) + [self.handle])
    Node.__init__(self, *zmq_args)
    self.last_value_accepted = None
    self.current_number_promised = None
    self.rejected = []
    self.missed_heartbeats = -1
    self.leader = None
    self.store_log = []

  def heartbeat_countdown_callback(self):
    if self.missed_heartbeats < 0:
      self.missed_heartbeats = 0
      return
    self.missed_heartbeats += 1
    self.log({'event': 'heartbeat from %s missed' % self.leader, 'node': self.name,
              'value': self.missed_heartbeats})
    if self.missed_heartbeats >= MAX_MISSED_HEARTBEATS:
      # Assume leader has failed
      self.leader = None
      self.missed_heartbeats = -1
    else:
      t = self.loop.time()
      self.loop.add_timeout(t+1, self.heartbeat_countdown_callback)

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    assert msg_frames[0] == self.name
    message = json.loads(msg_frames[2])
    with open(self.name+'.out', 'a') as outfh:
      print >> outfh, 'message:', message

    # Basic requests for store data
    if message['type'] == 'get':
      k = msg['key']
      v = self.store[k]
      self.log({'event': 'getting', 'node': self.name, 'key': k, 'value': v})
      self.req.send_json({'type': 'getResponse', 'id': msg['id'], 'value': v})
    elif message['type'] == 'paxos':
      try:
        self.handle_paxos(message)
      except Exception, e:
        with open(self.name+'.error', 'a') as outfh:
          print >> outfh, e
    elif message['type'] == 'hello':
      # should be the very first message we see
      self.respond_to_hello()
    else:
      self.log({'event': 'unknown', 'node': self.name})

    
  get_instnum = lambda n: int(n.split('-')[0])

  def handle_paxos(self, message):
    reply_dst = [message['src']]
    if self.leader and message['src'] != self.leader: # not from leader; reject
      self.log({'event': 'sending REJECT', 'node': self.name,
                'num': message['num'], 'dst': reply_dst})
      reply = self.paxos_message('REJECTED', message['num'], reply_dst,
                                 value=self.leader)
      self.req.send_json(reply)
      return
    if message['msg'] == 'PREPARE':
      assert not self.leader
      reply = None
      if self.current_number_promised == None:
        self.current_number_promised = message['num']
      if get_instnum(message['num']) < self.current_number_promised \
      or get_instnum(message['num']) <= get_instnum(self.store_log[-1][0]):
        if message['num'] not in self.rejected:
          self.log({'event': 'sending REJECT', 'node': self.name,
                    'num': message['num'], 'dst': reply_dst})
          reply = self.paxos_message('REJECTED', message['num'], reply_dst)
          self.req.send_json(reply)
          self.rejected.append(message['num'])
      else:
        self.current_number_promised = message['num']
        value = self.last_value_accepted
        self.log({'event': 'sending PROMISE', 'node': self.name,
                  'num': message['num'], 'value': value, 'dst': reply_dst})
        reply = self.paxos_message('PROMISE', message['num'], reply_dst,
                                   value=value)
        self.req.send_json(reply)
    elif message['msg'] == 'ACCEPT':
      reply = None
      if message['src'] != self.leader or \
        (message['num'] < self.current_number_promised):
        if message['num'] not in self.rejected:
          self.log({'event': 'sending REJECTED', 'node': self.name,
                    'num': message['num'], 'dst': reply_dst})
          reply = self.paxos_message('REJECTED', message['num'], reply_dst)
          self.req.send_json(reply)
          self.rejected.append(message['num'])
      else:
        self.last_value_accepted = message['value']
        self.log({'event': 'sending ACCEPTED', 'node': self.name,
                  'num': message['num'], 'value': message['value'], 
                  'dst': reply_dst})
        reply = self.paxos_message('ACCEPTED', message['num'], reply_dst,
                                   value=message['value'])
        self.req.send_json(reply)
    elif message['msg'] == 'LEARN':
      # Reset all Paxos instance variables and store new data appropriately
      value = message['value']
      self.last_value_accepted = None
      self.current_number_promised = None
      if type(value) == list:
        self.store[value[0]] = value[1]
        self.store_log.append((message['num'], message['value']))
      else:
        self.leader = value
        self.loop.add_callback(self.heartbeat_countdown_callback)

class Proposer(Node):
  def __init__(self, *zmq_args):
    zmq_args = tuple(list(zmq_args) + [self.handle])
    Node.__init__(self, *zmq_args)
    self.state = None
    self.received_promise = {}
    self.received_accepted = {}
    self.promise_rejected = {}
    self.accepted_rejected = {}
    self.current_proposal_num = None
    self.current_proposal_val = None
    self.consensus_on = None
    self.current_set_id = None
    self.leader = None #edited
    self.missed_heartbeats = -1
    self.store_log = []

  def set_to_promise(self):
    self.state = 'PROMISE'
    self.received_promise = {}
    self.recevied_accepted = {}
    self.promise_rejected = {}

  def set_to_accept(self):
    self.state = 'ACCEPTED'
    self.received_promise = {}
    self.recevied_accepted = {}
    self.accepted_rejected = {}

  def start_election(self):
    self.log({'event': 'starting election', 'node': self.name})
    self.log({'event': 'sending PREPARE', 'node': self.name,
              'name': num, 'dst': self.acceptors})
    self.req.send_json(self.paxos_message('PREPARE', num, self.acceptors))

  def heartbeat_countdown_callback(self):
    if self.missed_heartbeats < 0:
      self.missed_heartbeats = 0
      return
    self.missed_heartbeats += 1
    self.log({'event': 'heartbeat from %s missed' % self.leader,
              'node': self.name, 'value': self.missed_heartbeats})
    if self.missed_heartbeats >= MAX_MISSED_HEARTBEATS:
      # Assume leader has failed; start an election
      self.leader = None
      self.missed_heartbeats = -1
      self.start_election()
    else:
      t = self.loop.time()
      self.loop.add_timeout(t+1, self.heartbeat_countdown_callback)

  def reset_state(self):
    self.state = None
    self.received_promise = {}
    self.received_accepted = {}
    self.promise_rejected = {}
    self.accepted_rejected = {}
    self.current_proposal_num = None
    self.current_proposal_val = None

  def enter_heartbeat_mode(self, leader):
    assert leader != None
    assert leader != self.name
    self.leader = leader
    self.state = None
    self.received_promise = {}
    self.recevied_accepted = {}
    self.promise_rejected = {}
    self.accepted_rejected = {}
    self.loop.add_callback(self.heartbeat_countdown_callback)

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    assert msg_frames[0] == self.name
    message = json.loads(msg_frames[2])
    with open(self.name+'.out', 'a') as outfh:
      print >> outfh, 'message:', message

    # Basic requests for store data
    if message['type'] == 'get':
      k = message['key']
      v = self.store.get(k)
      if v:
        self.log({'event': 'getting', 'node': self.name, 'key': k, 'value': v})
        self.req.send_json({'type': 'getResponse', 'id': message['id'], 'value': v})
      else:
        self.log({'event': 'bad get (no such key)', 'node': self.name, 'key': k})
        self.req.send_json({'type': 'getResponse', 'id': message['id'], 
                            'error': 'bad get (no such key: %s)' % k})
    # Start a new round of Paxos if leader, otherwise
    # tell client who the leader is
    elif message['type'] == 'set':
      if self.leader == self.name:
        # TODO: eliminate proposal number in this case? Doesn't really seem
        # to be necessary...
        self.current_proposal_val = [message['key'], message['value']]
        num = message['id'] # num isn't related to ID, but it does help us
                            # choose a unique proposal number
        self.current_proposal_num = num
        self.current_set_id = message['id']
        self.log({'event': 'sending ACCEPT', 'node': self.name,
                  'num': num, 'dst': self.acceptors,
                  'value': self.current_proposal_value})
        self.req.send_json(self.paxos_message('PREPARE', num, self.acceptors,
                           value=self.current_proposal_value))
        self.set_to_accept()
      elif self.leader:
        self.req.send_json({'type': 'setResponse', 'id': message['id'],
                            'error': 'leader: '+self.leader})
    # Handle internal Paxos messages
    elif message['type'] == 'paxos':
      try:
        self.handle_paxos(message)
      except Exception, e:
        with open(self.name+'.error', 'a') as outfh:
          print >> outfh, e
    elif message['type'] == 'hello':
      # should be the very first message we see
      self.respond_to_hello()
      self.start_election()
    else:
      self.log({'event': 'unknown', 'node': self.name})

  def handle_paxos(self, message):
    # Only time we actually receive this message is if
    # we're in the middle of an election
    if message['msg'] == 'PROMISE':
      if message['num'] == self.current_proposal_num and self.state == 'PROMISE':
        #if message['value'] != None:
        #  self.current_proposal_val = message['value']
        self.received_promise[message['src']] = True
        if len(self.received_promise) > len(self.acceptors)/2:
          self.log({'event': 'sending ACCEPT', 'node': self.name,
                    'value': self.current_proposal_val, 'num': message['num'],
                    'dst': self.acceptors})
          reply = self.paxos_message('ACCEPT', message['num'], self.acceptors,
                                     value=self.current_proposal_val)
          self.req.send_json(reply)
          self.set_to_accept()
    elif message['msg'] == 'ACCEPTED':
      if message['num'] == self.current_proposal_num and self.state == 'ACCEPTED':
        self.received_accepted[message['src']] = True
        # Since any time you get an ACCEPT message you are guaranteed to be
        # the leader we could just have the same code, but it would be kind of
        # inefficient to send out info that we're the leader every time
        if len(self.received_accepted) > len(self.acceptors)/2:
          if not self.leader:
            assert message['value'] == self.name
            self.leader = self.name
            self.log({'event': 'accepted as leader', 'node': self.name})
          else:
            self.log({'event': 'accepted value', 'node': self.name,
                      'value': message['value'], 'id': self.current_set_id})
            self.store['accepted'] = message['value']
            self.req.send_json({'type': 'setResponse', 'id': self.current_set_id,
                                'value': message['value']})
          lrn = self.paxos_message('LEARN',self.current_set_id,
                                   self.proposers + self.acceptors,
                                   value=message['value'])
          self.req.send_json(lrn)
          self.reset_state()
    elif message['msg'] == 'REJECTED':
      if message['num'] == self.current_proposal_num:
        if self.state == 'PROMISE':
          self.promise_rejected[message['src']] = True
          if len(self.promise_rejected) > len(self.acceptors)/2:
            if not self.leader and 'value' in message:
              self.reset_state()
              self.enter_heartbeat_mode(message['value'])
            else:
              self.restart_round() # start from scratch
        elif self.state == 'ACCEPTED':
          self.accepted_rejected[message['src']] = True
          if len(self.accepted_rejected) > len(self.acceptors)/2:
            if not self.leader and 'value' in message:
              self.reset_state()
              self.enter_heartbeat_mode(message['value'])
            else:
              self.restart_round() # start from scratch
    elif message['msg'] == 'LEARN':
      # Reset all Paxos instance variables and store new data appropriately
      value = message['value']
      self.reset_state()
      if type(value) == list:
        self.store[value[0]] = value[1]
        self.store_log.append((message['num'], message['value']))
      else:
        self.enter_heartbeat_mode(value)
    #bw: BLOCK
    """
    elif message['msg'] == 'HEARTBEAT': 
      reply = self.paxos_message('ALIVE',None,message['src'])
      self.req.send_json(reply)
    """

  def restart_round(self):
    self.current_proposal_num += 2
    #bw: adding two cases, so we can call this once we have leaders too
    if self.leader:
      self.log({'event': 'sending ACCEPT', 'node': self.name,
                'num': self.current_proposal_num, 'dst': self.acceptors})
      reply = self.paxos_message('ACCEPT', self.current_proposal_num,
                                 self.acceptors, value=self.current_proposal_value)
      self.req.send_json(reply)
      self.set_to_accept()
    else:
      self.log({'event': 'sending PREPARE', 'node': self.name,
                'num': self.current_proposal_num, 'dst': self.acceptors})
      reply = self.paxos_message('PREPARE', self.current_proposal_num, 
                               self.acceptors)
      self.set_to_promise()

    self.req.send_json(reply)
    self.current_proposal_num += 2
    self.log({'event': 'sending PREPARE', 'node': self.name,
              'num': self.current_proposal_num, 'dst': self.acceptors})
    reply = self.paxos_message('PREPARE', self.current_proposal_num, 
                               self.acceptors)
    self.req.send_json(reply)
    self.set_to_promise()

  def send_heartbeats(self):
    if self.leader:
      msg = self.paxos_message('HEARTBEAT', None,
                               self.other_proposers+self.acceptors)
      self.req.send_json(msg)

  #bw: END BLOCK

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--pub-endpoint',
      dest='pub_endpoint', type=str,
      default='tcp://127.0.0.1:23310')
  parser.add_argument('--router-endpoint',
      dest='router_endpoint', type=str,
      default='tcp://127.0.0.1:23311')
  parser.add_argument('--node-name',
      dest='node_name', type=str,
      default='test_node')
  parser.add_argument('--peer-names',
      dest='peer_names', type=str,
      default='')
  parser.add_argument('--acceptor-names',
      dest='acceptors', type=str,
      default='')
  parser.add_argument('--proposer-names',
      dest='proposers', type=str,
      default='')
  parser.add_argument('--proposer',
      dest='is_proposer', action='store_true')
  parser.add_argument('--acceptor',
      dest='is_acceptor', action='store_true')
  parser.set_defaults(proposer=False, acceptor=False)
  args = parser.parse_args()
  args.peer_names = args.peer_names.split(',')
  args.proposers = args.proposers.split(',')
  args.acceptors = args.acceptors.split(',')

  if args.is_acceptor:
    Acceptor(args.node_name, args.pub_endpoint, args.router_endpoint, 
             args.peer_names, args.proposers, args.acceptors).start()
  elif args.is_proposer:
    Proposer(args.node_name, args.pub_endpoint, args.router_endpoint, 
             args.peer_names, args.proposers, args.acceptors).start()
  else:
    print >> sys.stderr, "Must set --proposer or --acceptor"
    sys.exit(-1)

