package store

import (
	"net/netip"
	"strings"
)

func isPod(name string) bool {
	return strings.HasPrefix(name, "//Pod")
}

func getWorkloadIPs(addresses [][]byte) []netip.Addr {
	if len(addresses) == 0 {
		return []netip.Addr{}
	}
	var ips []netip.Addr
	for _, ipBytes := range addresses {
		if ip, ok := netip.AddrFromSlice(ipBytes); ok {
			ips = append(ips, ip)
		}
	}
	return ips
}
