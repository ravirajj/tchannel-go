// Copyright (c) 2017 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tchannel

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
)

const (
	_defaultHealthCheckTimeout         = time.Second
	_defaultHealthCheckFailuresToClose = 5
)

// HealthCheckOptions are the parameters to configure active TChannel health
// checks. These are not intended to check application level health, but
// TCP connection health (similar to TCP keep-alives).
type HealthCheckOptions struct {
	// The period between health checks.
	Interval time.Duration

	// The timeout to use for a health check.
	// If no value is specified, it defaults to time.Second.
	Timeout time.Duration

	// FailuresToClose is the number of consecutive health check failures that
	// will cause this connection to be closed.
	// If no value is specified, it defaults to 5.
	FailuresToClose int
}

func (hco HealthCheckOptions) enabled() bool {
	return hco.Interval > 0
}

func (hco HealthCheckOptions) withDefaults() HealthCheckOptions {
	if hco.Timeout == 0 {
		hco.Timeout = _defaultHealthCheckTimeout
	}
	if hco.FailuresToClose == 0 {
		hco.FailuresToClose = _defaultHealthCheckFailuresToClose
	}
	return hco
}

// healthCheck will do periodic pings on the connection to check the state of the connection.
// We accept connID on the stack so can more easily debug panics or leaked goroutines.
func (c *Connection) healthCheck(connID uint32) {
	opts := c.opts.HealthChecks

	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()

	consecutiveFailures := 0
	for {
		select {
		case <-ticker.C:
		case <-c.healthCheckQuit:
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
		defer cancel()

		// TODO: Add log that we're performing a health check.
		err := c.ping(ctx)
		if err == nil {
			consecutiveFailures = 0
			continue
		}

		// If the health check failed because the connection is closed then
		// we don't need to do any extra logging or close the connection.
		if err == ErrInvalidConnectionState {
			return
		}

		c.connectionError("healthCheck", fmt.Errorf("healthCheck failed: %v", err))
		consecutiveFailures++
		if consecutiveFailures >= opts.FailuresToClose {
			c.close(LogFields{
				{"reason", "health check failure"},
				ErrField(err),
			}...)
			return
		}
	}
}

func (c *Connection) stophealthCheck() {
	if c.healthCheckStopped.Swap(true) {
		// Already been stopped.
		return
	}

	c.log.Debug("Stopping health checks.")
	close(c.healthCheckQuit)
}
