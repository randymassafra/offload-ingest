package licensing

import "runtime"

func runtimeGoroutines() int { return runtime.NumGoroutine() }
