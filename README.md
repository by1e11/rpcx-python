# rpcx-python
python server implementation of rpcx.io project

**Very early stage!!**

Only the most basic "hello world" function call tested

## TODO:
- [x] Build system
- [x] Implement basic function call
- [ ] Test Concurrent function call
- [x] Implement all message types
    - [x] msgpack
    - [ ] json
    - [ ] protobuf
    - [ ] raw
- [ ] Integrate with more tests from [rpcx-examples](https://github.com/rpcxio/rpcx-examples)
    - [ ] service discovery, reconnect
- [ ] Implement complete features specified by [rpcx.io](https://github.com/smallnest/rpcx) project
- [ ] Implement streaming RPC
    - [ ] streaming arguments to server
    - [ ] streaming response from server
- [ ] Implement client side

## Acknowledgement
* Borrowed test client from [](https://github.com/hyhkjiy/rpcx)
* Borrowed codes from [](https://github.com/uSpike/rpcx)