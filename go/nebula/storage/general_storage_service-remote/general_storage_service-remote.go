// Autogenerated by Thrift Compiler (facebook)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
// @generated

package main

import (
        "flag"
        "fmt"
        "math"
        "net"
        "net/url"
        "os"
        "strconv"
        "strings"
        thrift "github.com/facebook/fbthrift/thrift/lib/go/thrift"
        "../../github.com/vesoft-inc/nebula-clients/go/nebula/storage"
)

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  KVGetResponse get(KVGetRequest req)")
  fmt.Fprintln(os.Stderr, "  ExecResponse put(KVPutRequest req)")
  fmt.Fprintln(os.Stderr, "  ExecResponse remove(KVRemoveRequest req)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  var parsedUrl url.URL
  var trans thrift.Transport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Parse()
  
  if len(urlString) > 0 {
    parsedUrl, err := url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  if useHttp {
    trans, err = thrift.NewHTTPPostClient(parsedUrl.String())
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans, err = thrift.NewSocket(thrift.SocketAddr(net.JoinHostPort(host, portStr)))
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewFramedTransport(trans)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.ProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewCompactProtocolFactory()
    break
  case "simplejson":
    protocolFactory = thrift.NewSimpleJSONProtocolFactory()
    break
  case "json":
    protocolFactory = thrift.NewJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewBinaryProtocolFactoryDefault()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  client := storage.NewGeneralStorageServiceClientFactory(trans, protocolFactory)
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "get":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Get requires 1 args")
      flag.Usage()
    }
    arg368 := flag.Arg(1)
    mbTrans369 := thrift.NewMemoryBufferLen(len(arg368))
    defer mbTrans369.Close()
    _, err370 := mbTrans369.WriteString(arg368)
    if err370 != nil {
      Usage()
      return
    }
    factory371 := thrift.NewSimpleJSONProtocolFactory()
    jsProt372 := factory371.GetProtocol(mbTrans369)
    argvalue0 := storage.NewKVGetRequest()
    err373 := argvalue0.Read(jsProt372)
    if err373 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Get(value0))
    fmt.Print("\n")
    break
  case "put":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Put requires 1 args")
      flag.Usage()
    }
    arg374 := flag.Arg(1)
    mbTrans375 := thrift.NewMemoryBufferLen(len(arg374))
    defer mbTrans375.Close()
    _, err376 := mbTrans375.WriteString(arg374)
    if err376 != nil {
      Usage()
      return
    }
    factory377 := thrift.NewSimpleJSONProtocolFactory()
    jsProt378 := factory377.GetProtocol(mbTrans375)
    argvalue0 := storage.NewKVPutRequest()
    err379 := argvalue0.Read(jsProt378)
    if err379 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Put(value0))
    fmt.Print("\n")
    break
  case "remove":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "Remove requires 1 args")
      flag.Usage()
    }
    arg380 := flag.Arg(1)
    mbTrans381 := thrift.NewMemoryBufferLen(len(arg380))
    defer mbTrans381.Close()
    _, err382 := mbTrans381.WriteString(arg380)
    if err382 != nil {
      Usage()
      return
    }
    factory383 := thrift.NewSimpleJSONProtocolFactory()
    jsProt384 := factory383.GetProtocol(mbTrans381)
    argvalue0 := storage.NewKVRemoveRequest()
    err385 := argvalue0.Read(jsProt384)
    if err385 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.Remove(value0))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
