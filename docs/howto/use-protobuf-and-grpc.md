# Use Protobuf and gRPC

## References 

- ScalaPB : [link](https://scalapb.github.io/docs/grpc#project-setup)  
- ScalaPB protocol definition : [link](https://scalapb.github.io/docs/getting-started#defining-your-protocol-format)
- Protobuf format lists : [link](https://developers.google.com/protocol-buffers/docs/proto3#default)

## Service / Msessage definition
`file.proto`
```
syntax = "proto3";

package com.example.protos;

// The greeting service definition.
service Greeter {
  // Sends a greeting
  rpc SayHello (HelloRequest) returns (HelloReply) {}
}

// The request message containing the user's name.
message HelloRequest {
  string name = 1;
}

// The response message containing the greetings
message HelloReply {
  string message = 1;
}



```
