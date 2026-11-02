package server

// The enhanced-authentication seam: the broker owns the factory and the method it
// advertises, the connection (gmqx/conn) owns the packet flow of one exchange.

import "github.com/maxid/gmqx/hook"

// authExchange returns the exchange of method, or nil when this broker does not
// support the method. Nil is the answer for every method when
// Config.Authenticator is unset, for a method outside a non-empty
// Config.AuthMethods, and for a factory that declines the method or returns an
// exchange that reports a different one.
func (b *Broker) authExchange(method string) hook.AuthExchange {
	if method == "" || b.cfg.Authenticator == nil {
		return nil
	}
	if len(b.cfg.AuthMethods) > 0 {
		supported := false
		for _, advertised := range b.cfg.AuthMethods {
			if advertised == method {
				supported = true
				break
			}
		}
		if !supported {
			return nil
		}
	}
	exchange := b.cfg.Authenticator(method)
	if exchange == nil {
		return nil
	}
	if got := exchange.Method(); got != method {
		// A factory that answers one method with an exchange for another is a
		// configuration error: the broker would name the wrong method in the AUTH
		// packets, and the client's CONNACK would not match its CONNECT.
		if b.log != nil {
			b.log.Warn("gmqx authenticator returned an exchange for another method",
				"method", method, "exchange_method", got)
		}
		return nil
	}
	return exchange
}
