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
  def __init__(self, node_name, pub_endpoint, router_endpoint, 
               spammer, peer_names, handle_func):
    self.loop = ioloop.ZMQIOLoop.current()
    self.context = zmq.Context()

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
    self.spammer = spammer
    self.peer_names = peer_names
    self.acceptors = self.peer_names # TODO: more restrictive?

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

  def log(self, debug_info):
    self.req.json_send({'type': 'log', 'debug': debug_info})

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
    zmq_args.append(self.handle)
    Node.__init__(self, *zmq_args)
    self.last_value_accepted = None
    self.current_number_promised = None
    self.rejected = []
    self.failed = False

  def handle(self, msg_frame):
    assert len(msg_frame) == 3
    assert msg_frames[0] == self.name
    message = json.loads(msg_frames[2])

    # Basic requests for store data
    if message['type'] == 'get':
      k = msg['key']
      v = self.store[k]
      self.log({'event': 'getting', 'node': self.name, 'key': k, 'value': v})
      self.req.send_json({'type': 'getResponse', 'id': msg['id'], 'value': v})
    elif message['type'] == 'paxos':
      self.handle_paxos(message)
    elif msg['type'] == 'hello':
      # should be the very first message we see
      self.req.send_json({'type': 'hello', 'source': self.name})
    else:
      self.log({'event': 'unknown', 'node': self.name})

  def handle_paxos(self, message):
    if message['msg'] == 'PREPARE':
      reply = None
      if self.current_number_promised == None:
        self.current_number_promised = message['num']
      if message['num'] < self.current_number_promised:
        if message['num'] not in self.rejected:
          self.log({'event': 'rejecting', 'node': self.name,
                    'num': message['num'], 'dst': message['src']})
          reply = self.paxos_message('REJECTED', message['num'], message['src'])
          self.req.send_json(reply)
          self.rejected.append(message['num'])
      else:
        self.current_number_promised = message['num']
        value = self.last_value_accepted
        self.log({'event': 'promising', 'node': self.name,
                  'num': message['num'], 'value': value, 'dst': message['src']})
        reply = self.paxos_message('PROMISE', message['num'], message['src'],
                                   value=value)
        self.req.send_json(reply)
    elif message['type'] == 'ACCEPT':
      reply = None
      if message['num'] < self.current_number_promised:
        if message['num'] not in self.rejected:
          self.log({'event': 'rejecting', 'node': self.name,
                    'num': message['num'], 'dst': message['src']})
          reply = self.paxos_message('REJECTED', message['num'], message['src'])
          self.req.send_json(reply)
          self.rejected.append(message['num'])
      else:
        self.last_value_accepted = message['value']
        self.log({'event': 'accepted', 'node': self.name,
                  'num': message['num'], 'value': value, 'dst': message['src']})
        reply = self.paxos_message('ACCEPTED', message['num'], message['src'],
                                   value=message['value'])
        self.req.send_json(reply)


class Proposer:
  def __init__(self, *zmq_args):
    zmq_args.append(self.handle)
    Node.__init__(self, *zmq_args)
    self.state = None
    self.received_promise = {}
    self.received_accepted = {}
    self.promise_rejected = {}
    self.accepted_rejected = {}
    self.current_proposal_num = None
    self.orig_proposal_val = None
    self.current_proposal_val = None
    self.consensus_on = None
    self.failed = False

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

  def handle(self, msg_frame):
    assert len(msg_frame) == 3
    assert msg_frames[0] == self.name
    message = json.loads(msg_frames[2])

    # Basic requests for store data
    if message['type'] == 'get':
      k = msg['key']
      v = self.store.get(k)
      if v:
        self.log({'event': 'getting', 'node': self.name, 'key': k, 'value': v})
        self.req.send_json({'type': 'getResponse', 'id': msg['id'], 'value': v})
      else:
        self.log({'event': 'bad get (no such key)', 'node': self.name, 'key': k})
        self.req.send_json({'type': 'getResponse', 'id': msg['id'], 
                            'error': 'bad get (no such key: %s)' % k})
    # Start a new round of Paxos
    elif message['type'] == 'set':
      self.orig_proposal_val = message['value']
      self.current_proposal_val = message['value']
      num = 0 # TODO: Find a better way to implement this
      self.current_proposal_num = num
      self.log({'event': 'sending PREPARE', 'node': self.name,
                'num': num, 'dst': self.acceptors})
      self.req.send_json(self.paxos_message('PREPARE', num, self.acceptors))
      self.set_to_promise()
    # Handle internal Paxos messages
    elif message['type'] == 'paxos':
      self.handle_paxos(message)
    elif msg['type'] == 'hello':
      # should be the very first message we see
      self.req.send_json({'type': 'hello', 'source': self.name})
    else:
      self.log({'event': 'unknown', 'node': self.name})

  def handle_paxos(self, message):
    if message['msg'] == 'PROMISE':
      if message['num'] == self.current_proposal_num and self.state == 'PROMISE':
        if message['value'] != None:
          self.current_proposal_val = message['value']
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
        if len(self.received_accepted) > len(self.acceptors)/2:
          self.log({'event': 'accepted value', 'node': self.name,
                    'value': message['value']})
          self.store['accepted'] = message['value']
    elif message['msg'] == 'REJECTED':
      if message['num'] == self.current_proposal_num:
        if self.state == 'PROMISE':
          self.promise_rejected[message['src']] = True
          if len(self.promise_rejected) > len(self.acceptors)/2:
            self.restart_round() # start from scratch
        elif self.state == 'ACCEPTED':
          self.accepted_rejected[message['src']] = True
          if len(self.accepted_rejected) > len(self.acceptors)/2:
            self.restart_round() # start from scratch

  def restart_round(self):
    self.current_proposal_num += 2
    self.log({'event': 'sending PREPARE', 'node': self.name,
              'num': self.current_proposal_num, 'dst': self.acceptors})
    reply = self.paxos_message('PREPARE', self.current_proposal_num, 
                               self.acceptors)
    self.req.send_json(reply)
    self.set_to_promise()

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
  parser.add_argument('--spammer',
      dest='spammer', action='store_true')
  parser.set_defaults(spammer=False)
  parser.add_argument('--peer-names',
      dest='peer_names', type=str,
      default='')
  parser.add_argument('--proposer',
      dest='proposer', action='store_true')
  parser.add_argument('--acceptor',
      dest='acceptor', action='store_true')
  parser.set_defaults(proposer=False, acceptor=False)
  args = parser.parse_args()
  args.peer_names = args.peer_names.split(',')

  if args.acceptor:
    Acceptor(args.node_name, args.pub_endpoint, args.router_endpoint, 
             args.spammer, args.peer_names).start()
  elif args.proposer:
    Proposer(args.node_name, args.pub_endpoint, args.router_endpoint, 
             args.spammer, args.peer_names).start()
  else:
    print >> sys.stderr, "Must set --proposer or --acceptor"
    sys.exit(-1)

