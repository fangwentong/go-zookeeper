package zk

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
)

// DNSHostProvider is the default HostProvider. It currently matches
// the Java StaticHostProvider, resolving hosts from DNS once during
// the call to Init.  It could be easily extended to re-query DNS
// periodically or if there is trouble connecting.
type DNSHostProvider struct {
	mu         sync.Mutex // Protects everything, so we can add asynchronous updates later.
	servers    []inetAddress
	curr       int
	last       int
	lookupHost func(string) ([]string, error) // Override of net.LookupHost, for testing.
}

// Init is called first, with the servers specified in the connection
// string. It uses DNS to look up addresses for each server, then
// shuffles them all together.
func (hp *DNSHostProvider) Init(servers []string) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	addrs := make([]inetAddress, 0, len(servers))
	for _, server := range servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return err
		}
		addrs = append(addrs, inetAddress{host: host, port: port})
	}

	if len(addrs) == 0 {
		return fmt.Errorf("no hosts found for addresses %q", servers)
	}

	// shuffle the addresses
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})

	hp.servers = addrs
	hp.curr = -1
	hp.last = -1

	return nil
}

type inetAddress struct {
	host, port string
	resolved   bool
}

func (ia inetAddress) addr() string {
	return net.JoinHostPort(ia.host, ia.port)
}

func (hp *DNSHostProvider) resolve(addr inetAddress) (inetAddress, error) {
	if addr.resolved {
		return addr, nil
	}
	lookupHost := hp.lookupHost
	if lookupHost == nil {
		lookupHost = net.LookupHost
	}

	ips, err := lookupHost(addr.host)
	if err != nil {
		return addr, err
	}
	if len(ips) == 0 {
		return addr, fmt.Errorf("no hosts found for address %q", addr.host)
	}
	rand.Shuffle(len(ips), func(i, j int) {
		ips[i], ips[j] = ips[j], ips[i]
	})
	return inetAddress{
		host:     ips[0], // use the first IP
		port:     addr.port,
		resolved: true,
	}, nil
}

// Len returns the number of servers available
func (hp *DNSHostProvider) Len() int {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return len(hp.servers)
}

// Next returns the next server to connect to. retryStart will be true
// if we've looped through all known servers without Connected() being
// called.
func (hp *DNSHostProvider) Next() (server string, retryStart bool) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.curr = (hp.curr + 1) % len(hp.servers)
	retryStart = hp.curr == hp.last
	if hp.last == -1 {
		hp.last = 0
	}
	addr, err := hp.resolve(hp.servers[hp.curr])
	if err != nil {
		DefaultLogger.Printf("Error while resolving zk host %s: %s", hp.servers[hp.curr].host, err)
	}
	return addr.addr(), retryStart
}

// Connected notifies the HostProvider of a successful connection.
func (hp *DNSHostProvider) Connected() {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.last = hp.curr
}
