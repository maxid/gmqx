package gmqx

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func (c *client) readPacket() (byte, []byte, error) {
	header, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	remaining, err := readRemainingLength(c.r)
	if err != nil {
		return 0, nil, err
	}
	if remaining > c.broker.cfg.MaxPacketSize {
		return 0, nil, fmt.Errorf("%w: packet too large", ErrMalformedPacket)
	}
	payload := make([]byte, remaining)
	if _, err := io.ReadFull(c.r, payload); err != nil {
		return 0, nil, err
	}
	return header, payload, nil
}

func writePacket(w io.Writer, header byte, body []byte) error {
	buf := make([]byte, 0, 1+4+len(body))
	buf = append(buf, header)
	buf = appendRemainingLength(buf, len(body))
	buf = append(buf, body...)
	_, err := w.Write(buf)
	return err
}

func readRemainingLength(r *bufio.Reader) (int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(b&127) * multiplier
		if b&128 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("%w: malformed remaining length", ErrMalformedPacket)
}

func appendRemainingLength(dst []byte, n int) []byte {
	for {
		encoded := byte(n % 128)
		n /= 128
		if n > 0 {
			encoded |= 128
		}
		dst = append(dst, encoded)
		if n == 0 {
			return dst
		}
	}
}

func readUint16(b []byte) (uint16, []byte, error) {
	if len(b) < 2 {
		return 0, nil, ErrMalformedPacket
	}
	return binary.BigEndian.Uint16(b[:2]), b[2:], nil
}

func readString(b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, ErrMalformedPacket
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+n {
		return "", nil, ErrMalformedPacket
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}

func appendString(dst []byte, s string) []byte {
	dst = append(dst, byte(len(s)>>8), byte(len(s)))
	return append(dst, s...)
}

func parseConnect(payload []byte) (ConnectInfo, byte, connectProperties, error) {
	var info ConnectInfo
	var props connectProperties
	name, rest, err := readString(payload)
	if err != nil {
		return info, 0, props, err
	}
	if name != "MQTT" || len(rest) < 4 {
		return info, 0, props, ErrUnsupportedProtocol
	}
	level := rest[0]
	if level != 4 && level != 5 {
		return info, 0, props, ErrUnsupportedProtocol
	}
	flags := rest[1]
	keepAlive := binary.BigEndian.Uint16(rest[2:4])
	rest = rest[4:]
	if flags&0x01 != 0 {
		return info, 0, props, fmt.Errorf("%w: reserved connect flag", ErrMalformedPacket)
	}
	info.KeepAlive = keepAlive
	info.CleanSession = flags&0x02 != 0
	if level == 5 {
		block, next, err := readPropertyBlock(rest)
		if err != nil {
			return info, 0, props, err
		}
		if props, err = parseConnectProperties(block); err != nil {
			return info, 0, props, err
		}
		rest = next
	}
	clientID, rest, err := readString(rest)
	if err != nil {
		return info, 0, props, err
	}
	info.ClientID = clientID
	willFlag := flags&0x04 != 0
	if willFlag {
		willTopic, next, err := readString(rest)
		if err != nil {
			return info, 0, props, err
		}
		if level == 5 {
			if next, err = skipProperties(next); err != nil {
				return info, 0, props, err
			}
		}
		willPayload, next2, err := readBytes(next)
		if err != nil {
			return info, 0, props, err
		}
		info.WillTopic = willTopic
		info.WillPayload = willPayload
		rest = next2
	}
	if flags&0x80 != 0 {
		username, next, err := readString(rest)
		if err != nil {
			return info, 0, props, err
		}
		info.Username = username
		rest = next
	}
	if flags&0x40 != 0 {
		password, next, err := readBytes(rest)
		if err != nil {
			return info, 0, props, err
		}
		info.Password = password
		rest = next
	}
	return info, level, props, nil
}

// skipProperties reads an MQTT 5 property length and skips the properties.
func skipProperties(b []byte) ([]byte, error) {
	n, rest, err := readVariableByteInt(b)
	if err != nil {
		return nil, err
	}
	if len(rest) < n {
		return nil, ErrMalformedPacket
	}
	return rest[n:], nil
}

func readVariableByteInt(b []byte) (int, []byte, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		if len(b) < 1 {
			return 0, nil, ErrMalformedPacket
		}
		encoded := b[0]
		b = b[1:]
		value += int(encoded&127) * multiplier
		if encoded&128 == 0 {
			return value, b, nil
		}
		multiplier *= 128
	}
	return 0, nil, fmt.Errorf("%w: malformed variable byte integer", ErrMalformedPacket)
}

func readBytes(b []byte) ([]byte, []byte, error) {
	if len(b) < 2 {
		return nil, nil, ErrMalformedPacket
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+n {
		return nil, nil, ErrMalformedPacket
	}
	return b[2 : 2+n], b[2+n:], nil
}

var errNotImplemented = errors.New("not implemented")
