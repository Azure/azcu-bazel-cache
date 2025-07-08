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

	bazelazblob "github.com/Azure/azcu-bazel-cache"
	"google.golang.org/grpc"
)

func main() {
	var cfg bazelazblob.AzureBlobServerConfig

	cfg.CAS.ServiceURL = os.Getenv("AZURE_STORAGE_ACCOUNT")
	flag.StringVar(&cfg.CAS.ServiceURL, "cas-storage-account", cfg.CAS.ServiceURL, "Storage account name or service URL for CAS. Default: [AZURE_STORAGE_ACCOUNT]")

	cfg.AC.ServiceURL = os.Getenv("AZURE_STORAGE_ACCOUNT")
	flag.StringVar(&cfg.AC.ServiceURL, "ac-storage-account", cfg.AC.ServiceURL, "Storage account name or service URL for Action cache. Default: [AZURE_STORAGE_ACCOUNT]")

	cfg.CAS.Container = os.Getenv("AZURE_STORAGE_CONTAINER")
	flag.StringVar(&cfg.CAS.Container, "cas-storage-container", cfg.CAS.Container, "Storage account container for CAS. Default: [AZURE_STORAGE_CONTAINER]")

	cfg.AC.Container = os.Getenv("AZURE_STORAGE_CONTAINER")
	flag.StringVar(&cfg.AC.Container, "ac-storage-container", cfg.AC.Container, "Storage account container for Actionc ache. Default: [AZURE_STORAGE_CONTAINER]")

	flag.StringVar(&cfg.CAS.Prefix, "cas-blob-prefix", cfg.CAS.Prefix, "Blob name prefix for CAS: Default: cas")
	flag.StringVar(&cfg.AC.Prefix, "ac-blob-prefix", cfg.AC.Prefix, "Blob name prefix for Action cache: Default: ac")

	ignoreSameStorageFl := flag.Bool("ignore-same-storage-error", false, "Do not error out if AC and CAS are configured for the exact same storage settings")

	listenAddrFl := flag.String("address", "", "Address to serve on. Example: unix:///run/bazelazblob/server.sock or tcp://127.0.0.1:9000")

	flag.Parse()

	var exit bool
	if cfg.CAS.ServiceURL == "" && cfg.AC.ServiceURL == "" {
		fmt.Fprintln(os.Stderr, "--ac-storage-account and/or --cas-storage-account must be set")
		exit = true
	}

	if cfg.CAS.Container == "" && cfg.AC.Container == "" {
		fmt.Fprintln(os.Stderr, "--ac-storage-container and/or --cas-storage-container must be set")
		exit = true
	}

	if cfg.CAS.Equal(cfg.AC) && cfg.CAS.Prefix == "" {
		fmt.Fprintln(os.Stderr, "Storage is configured to go to the same place.")
		fmt.Fprintln(os.Stderr, "It is not recommended to run AC and CAS to the same storage paths")
		if !*ignoreSameStorageFl {
			exit = true
			fmt.Fprintln(os.Stderr, "Ignore this error by setting --ignore-same-storage-error")
		}
	}

	l, err := getListener(*listenAddrFl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error setting up server address:", err)
		exit = true
	}

	if exit {
		os.Exit(1)
	}

	if cfg.AC.ServiceURL == "" {
		cfg.AC.ServiceURL = cfg.CAS.ServiceURL
	}

	if cfg.CAS.ServiceURL == "" {
		cfg.CAS.ServiceURL = cfg.AC.ServiceURL
	}

	if cfg.AC.Container == "" {
		cfg.AC.Container = cfg.CAS.Container
	}

	if cfg.CAS.Container == "" {
		cfg.CAS.Container = cfg.AC.Container
	}

	if err := run(cfg, l); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func getListener(addr string) (net.Listener, error) {
	proto, addr, ok := strings.Cut(addr, "://")
	if !ok {
		return nil, fmt.Errorf("invalid addr format %q, must be in the form <proto>://<address>", addr)
	}
	return net.Listen(proto, addr)
}

func run(cfg bazelazblob.AzureBlobServerConfig, l net.Listener) error {
	defer l.Close()

	srv, err := bazelazblob.NewAzureBlobServer(cfg)
	if err != nil {
		return err
	}

	gsrv := grpc.NewServer()
	bazelazblob.RegisterAzureBlobServer(gsrv, srv)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		gsrv.Stop()
	}()

	return gsrv.Serve(l)
}
