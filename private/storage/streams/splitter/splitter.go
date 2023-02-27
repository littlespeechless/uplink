// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package splitter

import (
	"context"
	"crypto/rand"
	"io"

	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/buffer"
)

// Segment is an interface describing what operations a segment must provide
// to be uploaded to the network.
type Segment interface {
	// Begin returns a metaclient.BatchItem to begin the segment, either inline
	// or remote.
	Begin() metaclient.BatchItem

	// Position returns the segment position.
	Position() metaclient.SegmentPosition

	// Inline returns true if the segment is small enough to be inline.
	Inline() bool

	// Reader returns a fresh io.Reader that reads the data of the segment.
	Reader() io.Reader

	// EncryptETag encrypts the provided etag with the correct encryption
	// keys that the segment is using.
	EncryptETag(eTag []byte) ([]byte, error)

	// Finalize returns a SegmentInfo if the segment is done being read
	// from.
	Finalize() *SegmentInfo

	// DoneReading reports to the segment that we are no longer reading
	// with the provided error to report to writes.
	DoneReading(err error)
}

// SegmentInfo is information related to what is necessary to commit
// the segment.
type SegmentInfo struct {
	// Encryption contains the encryption parameters that will be stored
	// on the satellite.
	Encryption metaclient.SegmentEncryption

	// PlainSize is the plaintext number of bytes in the segment.
	PlainSize int64

	// EncryptedSize is the encrypted number of bytes in the segment.
	EncryptedSize int64
}

// Options controls parameters of how an incoming stream of bytes is
// split into segments, remote and inline.
type Options struct {
	// Split is the plaintext number of bytes to start new segments.
	Split int64

	// Minimum is the plaintext number of bytes necessary to create
	// a remote segment.
	Minimum int64

	// Params controls the encryption used on the plaintext bytes.
	Params storj.EncryptionParameters

	// Key is used to encrypt the encryption keys used to encrypt
	// the data.
	Key *storj.Key

	// PartNumber is the segment's part number if doing multipart
	// uploads, and 0 otherwise.
	PartNumber int32
}

// Splitter takes an incoming stream of bytes and splits it into
// encrypted segments.
type Splitter struct {
	// NewBackend lets one swap out the backend used to store segments
	// while they are being uploaded.
	NewBackend func() (buffer.Backend, error)

	split          *baseSplitter
	opts           Options
	maxSegmentSize int64
	index          int32
}

// New constructs a Splitter with the provided Options.
func New(opts Options) (*Splitter, error) {
	maxSegmentSize, err := encryption.CalcEncryptedSize(opts.Split, opts.Params)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return &Splitter{
		NewBackend: func() (buffer.Backend, error) { return buffer.NewMemoryBackend(0), nil },

		split:          newBaseSplitter(opts.Split, opts.Minimum),
		opts:           opts,
		maxSegmentSize: maxSegmentSize,
	}, nil
}

// Finish informs the Splitter that no more writes are coming, along with any error
// that may have caused the writes to stop.
func (e *Splitter) Finish(err error) { e.split.Finish(err) }

// Write appends data into the stream.
func (e *Splitter) Write(p []byte) (int, error) { return e.split.Write(p) }

// Next returns the next Segment split from the stream. If the stream is finished then
// it will return nil, nil.
func (e *Splitter) Next(ctx context.Context) (Segment, error) {
	position := metaclient.SegmentPosition{
		PartNumber: e.opts.PartNumber,
		Index:      e.index,
	}
	var contentKey storj.Key
	var keyNonce storj.Nonce

	// do all of the fallible actions before checking with the splitter
	nonce, err := nonceForPosition(position)
	if err != nil {
		return nil, err
	}
	if _, err := rand.Read(contentKey[:]); err != nil {
		return nil, errs.Wrap(err)
	}
	if _, err := rand.Read(keyNonce[:]); err != nil {
		return nil, errs.Wrap(err)
	}
	enc, err := encryption.NewEncrypter(e.opts.Params.CipherSuite, &contentKey, &nonce, int(e.opts.Params.BlockSize))
	if err != nil {
		return nil, errs.Wrap(err)
	}
	encKey, err := encryption.EncryptKey(&contentKey, e.opts.Params.CipherSuite, e.opts.Key, &keyNonce)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	backend, err := e.NewBackend()
	if err != nil {
		return nil, errs.Wrap(err)
	}

	buf := buffer.New(backend, e.opts.Minimum)
	wrc := encryption.TransformWriterPadded(buf, enc)
	encBuf := newEncryptedBuffer(buf, wrc)
	segEncryption := metaclient.SegmentEncryption{
		EncryptedKeyNonce: keyNonce,
		EncryptedKey:      encKey,
	}

	// check for the next segment/inline boundary. if an error, don't update any
	// local state.
	inline, eof, err := e.split.Next(ctx, encBuf)
	switch {
	case err != nil:
		return nil, errs.Wrap(err)

	case eof:
		return nil, nil

	case inline != nil:
		// encrypt the inline data, and update the internal state if it succeeds.
		encData, err := encryption.Encrypt(inline, e.opts.Params.CipherSuite, &contentKey, &nonce)
		if err != nil {
			return nil, errs.Wrap(err)
		}

		// everything fallible is done. update the internal state.
		e.index++

		return &splitterInline{
			position:   position,
			encryption: segEncryption,
			encParams:  e.opts.Params,
			contentKey: &contentKey,

			encData:   encData,
			plainSize: int64(len(inline)),
		}, nil

	default:
		// everything fallible is done. update the internal state.
		e.index++

		return &splitterSegment{
			position:   position,
			encryption: segEncryption,
			encParams:  e.opts.Params,
			contentKey: &contentKey,

			maxSegmentSize: e.maxSegmentSize,
			encTransformer: enc,
			encBuf:         encBuf,
		}, nil
	}
}