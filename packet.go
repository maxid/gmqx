package gmqx

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/maxid/gnety/codec/mqtt"
)

// MQTT wire format lives in gnety/codec/mqtt (codec layer). The
// shims below keep the broker code and tests stable during G1; call sites move
// to the codec package directly in G1b.

func (c *client) readPacket() (byte, []byte, error) {
	return mqtt.ReadPacket(c.r, c.broker.cfg.MaxPacketSize)
}

func writePacket(w io.Writer, header byte, body []byte) error {
	return mqtt.WritePacket(w, header, body)
}

func readRemainingLength(r *bufio.Reader) (int, error)  { return mqtt.ReadRemainingLength(r) }
func appendRemainingLength(dst []byte, n int) []byte    { return mqtt.AppendRemainingLength(dst, n) }
func readUint16(b []byte) (uint16, []byte, error)       { return mqtt.ReadUint16(b) }
func readString(b []byte) (string, []byte, error)       { return mqtt.ReadString(b) }
func appendString(dst []byte, s string) []byte          { return mqtt.AppendString(dst, s) }
func readBytes(b []byte) ([]byte, []byte, error)        { return mqtt.ReadBytes(b) }
func skipProperties(b []byte) ([]byte, error)           { return mqtt.SkipProperties(b) }
func readVariableByteInt(b []byte) (int, []byte, error) { return mqtt.ReadVariableByteInt(b) }

// parseConnect decodes a CONNECT payload into protocol-level fields. This is
// the MQTT state-machine boundary: wire parsing stays in gnety/codec/mqtt.
func parseConnect(payload []byte) (ConnectInfo, byte, connectProperties, error) {
	var info ConnectInfo
	var props connectProperties
	name, rest, err := readString(payload)
	if err != nil {
		return info, 0, props, err
	}
	if len(rest) < 4 {
		return info, 0, props, ErrUnsupportedProtocol
	}
	level := rest[0]
	// MQTT 3.1 uses protocol name "MQIsdp" with level 3; 3.1.1/5.0 use "MQTT".
	if !((name == "MQTT" && (level == 4 || level == 5)) || (name == "MQIsdp" && level == 3)) {
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
