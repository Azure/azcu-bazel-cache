package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	bazelazblob "github.com/cpuguy83/bazel-azblob"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	remoteAddrFl := flag.String("remote-address", "", "Remote address to connect to. Example: <proto>://<addr>, tcp://127.0.0.1:9000")
	addrFl := flag.String("address", "", "Address to listen on. Example: <proto>://<addr>, unix:///run/bazelazblob/proxy.sock")
	grpcInsecureFl := flag.Bool("grpc-insecure", false, "Set insecure credential option for grpc client connection")

	flag.Parse()

	var exit bool

	l, autoInsecure, err := getListener(*addrFl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error setting up server connection:", err)
		exit = true
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating Azure credential:", err)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithStreamInterceptor(bazelazblob.StreamClientAuthInterceptor(cred)),
		grpc.WithUnaryInterceptor(bazelazblob.UnaryClientAuthInterceptor(cred)),
	}

	if *grpcInsecureFl || autoInsecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	cc, err := grpc.NewClient(*remoteAddrFl, dialOpts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error setting up grpc client:", err)
		exit = true
	}

	if exit {
		if l != nil {
			l.Close()
		}
		os.Exit(1)
	}

	if err := run(cc, l); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func getListener(addr string) (net.Listener, bool, error) {
	proto, addr, ok := strings.Cut(addr, "://")
	if !ok {
		return nil, false, fmt.Errorf("invalid addr format %q, must be in the form <proto>://<address>", addr)
	}
	l, err := net.Listen(proto, addr)
	if err != nil {
		return nil, false, err
	}
	return l, proto == "unix", nil
}

func run(cc *grpc.ClientConn, l net.Listener) error {
	defer l.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	proxy := bazelazblob.NewProxy(bazelazblob.ProxyBackendFromGRPC(cc))
	gsrv := grpc.NewServer()
	bazelazblob.RegisterProxyServer(gsrv, proxy)

	go func() {
		<-ctx.Done()
		gsrv.Stop()
	}()
	return gsrv.Serve(l)
}
