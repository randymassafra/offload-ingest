package licensing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
)

// Fingerprint identifies the machine a licence is pinned to.
//
// It is a hash of two independent signals — a platform machine id and the
// hardware addresses of the physical network interfaces — so that neither one
// alone can move a licence. Cloning a disk to new hardware changes the MACs;
// swapping a NIC leaves the machine id. Requiring both to match would be
// brittle, so they are hashed together and the whole value is what gets pinned:
// change either and the licence stops matching, which is the intended strictness
// for a per-venue appliance licence.
//
// The value is deliberately not reversible. It goes in a licence file and in
// support tickets, so it must not carry a MAC address in the clear.
func Fingerprint() (string, error) {
	machine, err := machineID()
	if err != nil {
		return "", err
	}
	macs, err := hardwareAddrs()
	if err != nil {
		return "", err
	}
	if machine == "" && len(macs) == 0 {
		return "", fmt.Errorf("licensing: no stable hardware identity on this host")
	}

	h := sha256.New()
	// A domain separator, so this hash can never collide with some other use of
	// the same inputs elsewhere in the product.
	fmt.Fprintf(h, "offload-ingest/fingerprint/v1\x00%s\x00", machine)
	for _, m := range macs {
		fmt.Fprintf(h, "%s\x00", m)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// machineID reads the platform's own stable host identifier.
func machineID() (string, error) {
	switch runtime.GOOS {
	case "darwin":
		// IOPlatformUUID survives OS reinstalls and is stable per logic board.
		out, err := exec.Command("/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice").Output()
		if err != nil {
			return "", nil // fall back to MACs alone rather than failing closed
		}
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.Contains(line, "IOPlatformUUID") {
				continue
			}
			if _, v, ok := strings.Cut(line, "="); ok {
				return strings.Trim(strings.TrimSpace(v), `"`), nil
			}
		}
		return "", nil
	case "linux":
		// /etc/machine-id is the systemd standard; the dbus copy is the
		// fallback on hosts that predate it or mount /etc read-only.
		for _, p := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
			if b, err := os.ReadFile(p); err == nil {
				if id := strings.TrimSpace(string(b)); id != "" {
					return id, nil
				}
			}
		}
		return "", nil
	case "windows":
		out, err := exec.Command("cmd", "/C", "wmic csproduct get UUID").Output()
		if err != nil {
			return "", nil
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		if len(lines) > 1 {
			return strings.TrimSpace(lines[len(lines)-1]), nil
		}
		return "", nil
	default:
		return "", nil
	}
}

// hardwareAddrs returns the physical interfaces' MACs, sorted.
//
// Loopback, down and virtual interfaces are skipped: a Docker bridge or a VPN
// tap appears and disappears with workload, and including one would make the
// fingerprint depend on whether a container happened to be running.
func hardwareAddrs() ([]string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, i := range ifaces {
		if i.Flags&net.FlagLoopback != 0 || len(i.HardwareAddr) == 0 {
			continue
		}
		if isVirtual(i.Name) {
			continue
		}
		out = append(out, i.HardwareAddr.String())
	}
	sort.Strings(out)
	return out, nil
}

// virtualPrefixes are interface names that come and go with workload rather
// than describing the machine.
var virtualPrefixes = []string{
	"docker", "br-", "veth", "virbr", "vmnet", "vboxnet", "tun", "tap",
	"utun", "awdl", "llw", "bridge", "zt", "wg", "cni", "flannel", "kube",
}

func isVirtual(name string) bool {
	n := strings.ToLower(name)
	for _, p := range virtualPrefixes {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	return false
}
