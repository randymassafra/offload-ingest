package metrics

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// HostSample is one reading of the edge appliance's resources.
type HostSample struct {
	CPUPercent float64 // 0..100 across all cores
	MemUsed    float64 // bytes
	MemTotal   float64 // bytes
	LoadAvg1   float64
	// OK is false when the platform could not be read at all.
	OK bool
}

// HostSampler reads host resources.
//
// An interface because the production target is a Linux Minisforum box and
// development happens on macOS, and because a fake sampler is the only way to
// test the alerting thresholds without pinning a real CPU. The Linux
// implementation reads /proc directly rather than taking a dependency: the two
// files it needs are stable kernel ABI, and adding a cross-platform metrics
// library to read them would be the largest dependency in the module.
type HostSampler interface {
	Sample() HostSample
}

// NewHostSampler returns the sampler for this platform.
func NewHostSampler() HostSampler {
	switch runtime.GOOS {
	case "linux":
		return &procSampler{}
	case "darwin":
		return &darwinSampler{}
	default:
		return unsupportedSampler{}
	}
}

type unsupportedSampler struct{}

func (unsupportedSampler) Sample() HostSample { return HostSample{} }

// --- linux ------------------------------------------------------------------

// procSampler reads /proc/stat and /proc/meminfo.
type procSampler struct {
	lastIdle, lastTotal uint64
}

func (p *procSampler) Sample() HostSample {
	s := HostSample{}
	if idle, total, ok := readProcStat(); ok {
		// CPU percentage is a rate, so the first sample can only establish a
		// baseline — reporting a number from one reading would be the average
		// since boot, not now.
		if p.lastTotal > 0 && total > p.lastTotal {
			dIdle := float64(idle - p.lastIdle)
			dTotal := float64(total - p.lastTotal)
			s.CPUPercent = 100 * (1 - dIdle/dTotal)
			s.OK = true
		}
		p.lastIdle, p.lastTotal = idle, total
	}
	if used, total, ok := readMemInfo(); ok {
		s.MemUsed, s.MemTotal, s.OK = used, total, true
	}
	if l, ok := readLoadAvg(); ok {
		s.LoadAvg1 = l
	}
	return s
}

func readProcStat() (idle, total uint64, ok bool) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:]
		for i, f := range fields {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				continue
			}
			total += v
			// Fields 3 and 4 are idle and iowait; both are time not spent
			// working, and counting iowait as busy would make a disk-bound
			// appliance look CPU-saturated.
			if i == 3 || i == 4 {
				idle += v
			}
		}
		return idle, total, true
	}
	return 0, 0, false
}

func readMemInfo() (used, total float64, ok bool) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	var memTotal, memAvailable float64
	for _, line := range strings.Split(string(b), "\n") {
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		kb, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		switch key {
		case "MemTotal":
			memTotal = kb * 1024
		case "MemAvailable":
			// MemAvailable, not MemFree: page cache is reclaimable, and
			// reporting it as used makes every healthy Linux box look full.
			memAvailable = kb * 1024
		}
	}
	if memTotal == 0 {
		return 0, 0, false
	}
	return memTotal - memAvailable, memTotal, true
}

func readLoadAvg() (float64, bool) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	return v, err == nil
}

// --- darwin -----------------------------------------------------------------

// darwinSampler exists so the dashboard is developable on a Mac. It shells out
// rather than using cgo, which keeps cross-compilation for the linux/amd64
// deployment target working with no build tags.
type darwinSampler struct{}

func (darwinSampler) Sample() HostSample {
	s := HostSample{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if out, err := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "hw.memsize").Output(); err == nil {
		if v, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); err == nil {
			s.MemTotal, s.OK = v, true
		}
	}
	if out, err := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "vm.loadavg").Output(); err == nil {
		// Format: "{ 2.15 2.40 2.60 }"
		fields := strings.Fields(strings.Trim(strings.TrimSpace(string(out)), "{} "))
		if len(fields) > 0 {
			if v, err := strconv.ParseFloat(fields[0], 64); err == nil {
				s.LoadAvg1 = v
			}
		}
	}
	// Approximate CPU from load average against core count. `top` would be
	// exact but costs a second of sampling per reading; this is a development
	// convenience, and the production path reads /proc.
	if cores := runtime.NumCPU(); cores > 0 && s.LoadAvg1 > 0 {
		p := 100 * s.LoadAvg1 / float64(cores)
		if p > 100 {
			p = 100
		}
		s.CPUPercent, s.OK = p, true
	}
	if s.MemTotal > 0 {
		if out, err := exec.CommandContext(ctx, "/usr/bin/vm_stat").Output(); err == nil {
			if used, ok := parseVMStat(string(out)); ok {
				s.MemUsed = used
			}
		}
	}
	return s
}

// parseVMStat totals the page classes that genuinely occupy memory.
func parseVMStat(out string) (float64, bool) {
	pageSize := 4096.0
	var active, wired, compressed float64
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "page size of") {
			for _, f := range strings.Fields(line) {
				if v, err := strconv.ParseFloat(f, 64); err == nil && v > 512 {
					pageSize = v
					break
				}
			}
			continue
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		v, err := strconv.ParseFloat(strings.Trim(strings.TrimSpace(value), "."), 64)
		if err != nil {
			continue
		}
		switch strings.TrimSpace(key) {
		case "Pages active":
			active = v
		case "Pages wired down":
			wired = v
		case "Pages occupied by compressor":
			compressed = v
		}
	}
	if active+wired == 0 {
		return 0, false
	}
	return (active + wired + compressed) * pageSize, true
}

// --- collector --------------------------------------------------------------

// StartHostCollector samples host resources on an interval until ctx ends.
//
// Process-level figures (RSS, goroutines) are always available and are recorded
// even when the host sampler cannot read the platform, because "the box is
// fine but our process is leaking" is a distinct and more common failure than
// the host running out of memory.
func (r *Registry) StartHostCollector(ctx context.Context, sampler HostSampler, every time.Duration) {
	if sampler == nil {
		sampler = NewHostSampler()
	}
	if every <= 0 {
		every = 5 * time.Second
	}
	collect := func() {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		r.Host.ProcessRSS.Set(float64(mem.Sys))
		r.Host.Goroutines.Set(float64(runtime.NumGoroutine()))

		s := sampler.Sample()
		r.Host.Available.Set(s.OK)
		if !s.OK {
			return
		}
		r.Host.CPUPercent.Set(s.CPUPercent)
		r.Host.MemUsed.Set(s.MemUsed)
		r.Host.MemTotal.Set(s.MemTotal)
		r.Host.LoadAvg1.Set(s.LoadAvg1)
		if s.MemTotal > 0 {
			r.Host.MemPercent.Set(100 * s.MemUsed / s.MemTotal)
			r.Host.MemSeries.Add(100 * s.MemUsed / s.MemTotal)
		}
		r.Host.CPUSeries.Add(s.CPUPercent)
	}
	go func() {
		collect()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				collect()
			}
		}
	}()
}
