package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"rtmp-http-tunnel/pkg/client"
	"rtmp-http-tunnel/pkg/common"
	"rtmp-http-tunnel/pkg/server"
)

func main() {
	// 1. Define CLI flags
	configPath := flag.String("config", "config.json", "Path to the configuration file")
	modeOverride := flag.String("mode", "", "Override mode: 'client' or 'server'")
	flag.Parse()

	// 2. Load Configuration
	cfg, err := common.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("[Main] Failed to load config file (%s): %v", *configPath, err)
	}

	// Apply mode override if provided via CLI flag
	mode := cfg.Mode
	if *modeOverride != "" {
		mode = *modeOverride
	}
	if err := cfg.Validate(mode); err != nil {
		log.Fatalf("[Main] Invalid configuration: %v", err)
	}

	log.Printf("[Main] Running in '%s' mode...", mode)

	// 3. Graceful Shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("[Main] Received signal %v. Initiating graceful shutdown...", sig)
		cancel()
	}()

	// 4. Run Mode
	switch mode {
	case "client":
		c := client.NewClient(&cfg.Client)
		if err := c.Start(ctx); err != nil && err != context.Canceled {
			log.Fatalf("[Main] Client exited with fatal error: %v", err)
		}
		log.Println("[Main] Client shut down gracefully")

	case "server":
		s := server.NewServer(&cfg.Server)
		if err := s.Start(ctx); err != nil && err != context.Canceled {
			log.Fatalf("[Main] Server exited with fatal error: %v", err)
		}
		log.Println("[Main] Server shut down gracefully")

	default:
		log.Fatalf("[Main] Invalid run mode: %s. Must be 'client' or 'server'", mode)
	}
}
