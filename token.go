/*
 * Copyright (c) 2021 IBM Corp and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Allan Stockdill-Mander
 */

package mqtt

import (
	"sync"
	"time"

	"github.com/harri2012/mqtt.go/packets"
)

// PacketAndToken is a struct that contains both a ControlPacket and a
// Token. This struct is passed via channels between the client interface
// code and the underlying code responsible for sending and receiving
// MQTT messages.
type PacketAndToken struct {
	p packets.ControlPacket
	t tokenCompletor
}

// Token defines the interface for the tokens used to indicate when
// actions have completed.
type Token interface {
	// Wait will wait indefinitely for the Token to complete, ie the Publish
	// to be sent and confirmed receipt from the broker.
	Wait() bool

	// WaitTimeout takes a time.Duration to wait for the flow associated with the
	// Token to complete, returns true if it returned before the timeout or
	// returns false if the timeout occurred. In the case of a timeout the Token
	// does not have an error set in case the caller wishes to wait again.
	WaitTimeout(time.Duration) bool

	// Done returns a channel that is closed when the flow associated
	// with the Token completes. Clients should call Error after the
	// channel is closed to check if the flow completed successfully.
	//
	// Done is provided for use in select statements. Simple use cases may
	// use Wait or WaitTimeout.
	Done() <-chan struct{}

	Error() error
	// 获取Token生成时的时间戳，毫秒
	Timestamp() int64
	// 设置Token完成时的回调函数，请不要使用耗时较长的函数
	SetCallback(func(Token))
}

type TokenErrorSetter interface {
	setError(error)
}

type tokenCompletor interface {
	Token
	TokenErrorSetter
	flowComplete()
}

type baseToken struct {
	m        sync.RWMutex
	complete chan struct{}
	err      error
	// 是否完成，0：未完成，1：完成
	status uint
	// token创建的时间戳，毫秒
	tm int64
	// newToken方法生成的具体Token的引用，用于callback
	real    Token
	callbak func(Token)
}

// Wait implements the Token Wait method.
func (b *baseToken) Wait() bool {
	<-b.complete
	return true
}

// WaitTimeout implements the Token WaitTimeout method.
func (b *baseToken) WaitTimeout(d time.Duration) bool {
	timer := time.NewTimer(d)
	select {
	case <-b.complete:
		if !timer.Stop() {
			<-timer.C
		}
		return true
	case <-timer.C:
	}

	return false
}

// Done implements the Token Done method.
func (b *baseToken) Done() <-chan struct{} {
	return b.complete
}

func (b *baseToken) flowComplete() {
	select {
	case <-b.complete:
	default:
		close(b.complete)
	}

	b.m.Lock()
	b.status = 1
	call := b.callbak != nil && b.real != nil
	b.m.Unlock()
	if call {
		b.callbak(b.real)
	}
}

func (b *baseToken) Error() error {
	b.m.RLock()
	defer b.m.RUnlock()
	return b.err
}

func (b *baseToken) setError(e error) {
	b.m.Lock()
	b.err = e
	b.m.Unlock()
	b.flowComplete()
}

func (b *baseToken) Timestamp() int64 {
	return b.tm
}

func (b *baseToken) SetCallback(f func(Token)) {
	b.m.Lock()
	b.callbak = f
	call := b.status != 0 && b.real != nil
	b.m.Unlock()
	if call {
		f(b.real)
	}
}

func newToken(tType byte) tokenCompletor {
	switch tType {
	case packets.Connect:
		t := &ConnectToken{baseToken: baseToken{complete: make(chan struct{}), tm: time.Now().UnixMilli()}}
		t.baseToken.real = t
		return t
	case packets.Subscribe:
		t := &SubscribeToken{baseToken: baseToken{complete: make(chan struct{}), tm: time.Now().UnixMilli()}, subResult: make(map[string]byte)}
		t.baseToken.real = t
		return t
	case packets.Publish:
		t := &PublishToken{baseToken: baseToken{complete: make(chan struct{}), tm: time.Now().UnixMilli()}}
		t.baseToken.real = t
		return t
	case packets.Unsubscribe:
		t := &UnsubscribeToken{baseToken: baseToken{complete: make(chan struct{}), tm: time.Now().UnixMilli()}}
		t.baseToken.real = t
		return t
	case packets.Disconnect:
		t := &DisconnectToken{baseToken: baseToken{complete: make(chan struct{}), tm: time.Now().UnixMilli()}}
		t.baseToken.real = t
		return t
	}
	return nil
}

// ConnectToken is an extension of Token containing the extra fields
// required to provide information about calls to Connect()
type ConnectToken struct {
	baseToken
	returnCode     byte
	sessionPresent bool
}

// ReturnCode returns the acknowledgement code in the connack sent
// in response to a Connect()
func (c *ConnectToken) ReturnCode() byte {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.returnCode
}

// SessionPresent returns a bool representing the value of the
// session present field in the connack sent in response to a Connect()
func (c *ConnectToken) SessionPresent() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.sessionPresent
}

// PublishToken is an extension of Token containing the extra fields
// required to provide information about calls to Publish()
type PublishToken struct {
	baseToken
	messageID uint16
}

// MessageID returns the MQTT message ID that was assigned to the
// Publish packet when it was sent to the broker
func (p *PublishToken) MessageID() uint16 {
	return p.messageID
}

// SubscribeToken is an extension of Token containing the extra fields
// required to provide information about calls to Subscribe()
type SubscribeToken struct {
	baseToken
	subs      []string
	subResult map[string]byte
	messageID uint16
}

// Result returns a map of topics that were subscribed to along with
// the matching return code from the broker. This is either the Qos
// value of the subscription or an error code.
func (s *SubscribeToken) Result() map[string]byte {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.subResult
}

// UnsubscribeToken is an extension of Token containing the extra fields
// required to provide information about calls to Unsubscribe()
type UnsubscribeToken struct {
	baseToken
	messageID uint16
}

// DisconnectToken is an extension of Token containing the extra fields
// required to provide information about calls to Disconnect()
type DisconnectToken struct {
	baseToken
}
