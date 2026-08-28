import sys, zmq


ctx = zmq.Context()

dealer = ctx.socket(zmq.DEALER)
dealer.setsockopt(zmq.IDENTITY, b"py-client")
dealer.connect(sys.argv[1])
dealer.send_multipart([b"one", b"two"])
assert dealer.recv_multipart() == [b"reply"]

sub = ctx.socket(zmq.SUB)
sub.setsockopt(zmq.SUBSCRIBE, b"topic")
sub.connect(sys.argv[2])
assert sub.recv_multipart() == [b"topic", b"payload"]

req = ctx.socket(zmq.REQ)
req.connect(sys.argv[3])
req.send(b"heartbeat")
assert req.recv() == b"heartbeat"

dealer.close()
sub.close()
req.close()
ctx.term()
