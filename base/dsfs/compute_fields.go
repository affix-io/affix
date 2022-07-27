package dsfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/affix-io/affix/event"
	"github.com/affix-io/dataset"
	"github.com/affix-io/dataset/dsio"
	"github.com/affix-io/dataset/dsstats"
	"github.com/affix-io/jsonschema"
	"github.com/affix-io/qfs"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
)

type computeFieldsFile struct {
	*sync.Mutex

	publisher event.Publisher // optional bus to publish progress events to
	pk        crypto.PrivKey  // key for signing version
	sw        *SaveSwitches

	ds, prev *dataset.Dataset

	// body statistics accumulator
	acc *dsstats.Accumulator

	// buffer of entries for diffing small datasets. will be set to nil if
	// body reads more than BodySizeSmallEnoughToDiff bytes
	diffMessageBuf *dsio.EntryBuffer

	bodySize   int64 // copy provided body file .Size() method
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	teeReader  *dsio.TrackedReader
	done       chan error

	batches int
}

var (
	_ doneProcessingFile = (*computeFieldsFile)(nil)
	_ statsComponentFile = (*computeFieldsFile)(nil)
	_ qfs.SizeFile       = (*computeFieldsFile)(nil)
)

func newComputeFieldsFile(
	ctx context.Context,
	pub event.Publisher,
	pk crypto.PrivKey,
	ds *dataset.Dataset,
	prev *dataset.Dataset,
	sw *SaveSwitches) (qfs.File, error) {
	var (
		bf     = ds.BodyFile()
		bfPrev qfs.File
	)

	if prev != nil {
		bfPrev = prev.BodyFile()
	}
	if bf == nil && bfPrev == nil {
		return nil, fmt.Errorf("bodyfile or previous bodyfile needed")
	} else if bf == nil {
		// TODO(dustmop): If no bf provided, we're assuming that the body is the same as it
		// was in the previous commit. In this case, we shouldn't be recalculating the
		// structure (err count, depth, checksum, length) we should just copy it from the
		// previous version.
		bf = bfPrev
	}

	bodySize := int64(-1)
	if sf, ok := bf.(qfs.SizeFile); ok {
		bodySize = sf.Size()
	}

	pr, pw := io.Pipe()
	tr := io.TeeReader(bf, pw)
	sw.bodyAct = BodyDefault

	cff := &computeFieldsFile{
		Mutex:      &sync.Mutex{},
		publisher:  pub,
		pk:         pk,
		sw:         sw,
		ds:         ds,
		prev:       prev,
		bodySize:   bodySize,
		pipeReader: pr,
		pipeWriter: pw,
		teeReader:  dsio.NewTrackedReader(tr),
		done:       make(chan error),
	}

	go cff.handleRows(ctx)

	return cff, nil
}

func (cff *computeFieldsFile) FileName() string {
	return fmt.Sprintf("/%s", cff.ds.Structure.BodyFilename())
}

func (cff *computeFieldsFile) FullPath() string {
	return fmt.Sprintf("/%s", cff.ds.Structure.BodyFilename())
}

func (cff *computeFieldsFile) IsDirectory() bool {
	return false
}

func (cff *computeFieldsFile) MediaType() string {
	panic("cannot call MediaType of computeFieldsFile")
}

func (cff *computeFieldsFile) ModTime() time.Time {
	panic("cannot call ModTime of computeFieldsFile")
}

func (cff *computeFieldsFile) Size() int64 {
	return cff.bodySize
}

func (cff *computeFieldsFile) NextFile() (qfs.File, error) {
	return nil, qfs.ErrNotDirectory
}

func (cff *computeFieldsFile) Read(p []byte) (n int, err error) {
	n, err = cff.teeReader.Read(p)

	if err != nil && err.Error() == "EOF" {
		cff.pipeWriter.Close()
	}

	return n, err
}

func (cff *computeFieldsFile) Close() error {
	cff.pipeWriter.Close()
	return nil
}

type doneProcessingFile interface {
	DoneProcessing() <-chan error
}

func (cff *computeFieldsFile) DoneProcessing() <-chan error {
	return cff.done
}

type statsComponentFile interface {
	StatsComponent() (*dataset.Stats, error)
}

func (cff *computeFieldsFile) StatsComponent() (*dataset.Stats, error) {
	return &dataset.Stats{
		affix: dataset.KindStats.String(),
		Stats: dsstats.ToMap(cff.acc),
	}, nil
}

func (cff *computeFieldsFile) handleRows(ctx context.Context) {
	var (
		batchBuf      *dsio.EntryBuffer
		st            = cff.ds.Structure
		valErrorCount = 0
		entries       = 0
		depth         = 0
	)

	r, err := dsio.NewEntryReader(st, cff.pipeReader)
	if err != nil {
		log.Debugf("creating entry reader: %s", err)
		cff.done <- fmt.Errorf("creating entry reader: %w", err)
		return
	}

	cff.Lock()
	cff.acc = dsstats.NewAccumulator(st)
	cff.Unlock()

	jsch, err := st.JSONSchema()
	if err != nil {
		cff.done <- err
		return
	}

	batchBuf, err = dsio.NewEntryBuffer(&dataset.Structure{
		Format: "json",
		Schema: st.Schema,
	})
	if err != nil {
		cff.done <- fmt.Errorf("allocating data buffer: %w", err)
		return
	}

	cff.diffMessageBuf, err = dsio.NewEntryBuffer(&dataset.Structure{
		Format: "json",
		Schema: st.Schema,
	})
	if err != nil {
		cff.done <- fmt.Errorf("allocating data buffer: %w", err)
		return
	}

	if cff.publisher != nil {
		// publish here so we know that if the user sees the "processing body file"
		// message, we know that a compute-fields-file has made it all the way through
		// setup
		go func() {
			completion := 0.1
			if cff.bodySize >= 0 {
				completion = float64(cff.teeReader.BytesRead()) / float64(cff.bodySize)
			}
			evtErr := cff.publisher.Publish(ctx, event.ETDatasetSaveProgress, event.DsSaveEvent{
				Username:   cff.ds.Peername,
				Name:       cff.ds.Name,
				Message:    "processing body file",
				Completion: completion,
			})
			if evtErr != nil {
				log.Debugw("ignored error while publishing save progress", "evtErr", evtErr)
			}
		}()
	}

	go func() {
		err = dsio.EachEntry(r, func(i int, ent dsio.Entry, err error) error {
			if err != nil {
				return fmt.Errorf("reading row %d: %w", i, err)
			}

			// get the depth of this entry, update depth if larger
			if d := getDepth(ent.Value); d > depth {
				depth = d
			}
			entries++
			if err := cff.acc.WriteEntry(ent); err != nil {
				return err
			}

			if i%batchSize == 0 && i != 0 {
				numValErrs, flushErr := cff.flushBatch(ctx, batchBuf, st, jsch)
				if flushErr != nil {
					log.Debugf("error flushing batch while reading; %s", flushErr)
					return flushErr
				}
				valErrorCount += numValErrs
				var bufErr error
				batchBuf, bufErr = dsio.NewEntryBuffer(&dataset.Structure{
					Format: "json",
					Schema: st.Schema,
				})
				if bufErr != nil {
					log.Debugf("error allocating data buffer; %s", bufErr)
					return fmt.Errorf("allocating data buffer: %w", bufErr)
				}
			}

			err = batchBuf.WriteEntry(ent)
			if err != nil {
				log.Debugf("error writing entry row: %s", err)
				return fmt.Errorf("writing row %d: %w", i, err)
			}

			if cff.diffMessageBuf != nil {
				if err = cff.diffMessageBuf.WriteEntry(ent); err != nil {
					log.Debugf("error writing diff message buffer row: %s", err)
					return err
				}
			}

			return nil
		})

		if err != nil {
			log.Debugf("error processing body data: %s", err)
			cff.done <- fmt.Errorf("processing body data: %w", err)
			return
		}

		log.Debugf("read all %d entries", entries)
		numValErrs, err := cff.flushBatch(ctx, batchBuf, st, jsch)
		if err != nil {
			log.Debugf("flushing final batch: %s", err)
			cff.done <- err
			return
		}
		valErrorCount += numValErrs

		cff.Lock()
		defer cff.Unlock()
		log.Debugw("determined structure values", "errCount", valErrorCount, "entries", entries, "depth", depth, "bytecount", cff.teeReader.BytesRead())
		cff.ds.Structure.ErrCount = valErrorCount
		cff.ds.Structure.Entries = entries
		cff.ds.Structure.Depth = depth + 1 // need to add one for the original enclosure
		cff.ds.Structure.Length = cff.teeReader.BytesRead()

		// as we're using a manual setup on the EntryReader we also need
		// to manually close the accumulator to finalize results before write
		cff.acc.Close()

		// If the body exists and is small enough, deserialize it and assign it
		if cff.diffMessageBuf != nil {
			if err := cff.diffMessageBuf.Close(); err != nil {
				log.Debugf("inlining buffered body data: %s", err)
				cff.done <- fmt.Errorf("closing body data buffer: %w", err)
			}
			if cff.ds.Body, err = dsio.ReadAll(cff.diffMessageBuf); err != nil {
				log.Debugf("inlining buffered body data: %s", err)
				cff.done <- fmt.Errorf("inlining buffered body data: %w", err)
				return
			}
		}

		cff.done <- nil
		log.Debugf("done handling structured entries")
	}()

	return
}

func (cff *computeFieldsFile) flushBatch(ctx context.Context, buf *dsio.EntryBuffer, st *dataset.Structure, jsch *jsonschema.Schema) (int, error) {
	log.Debugf("flushing batch %d", cff.batches)
	cff.batches++

	if cff.diffMessageBuf != nil && cff.teeReader.BytesRead() > BodySizeSmallEnoughToDiff {
		log.Debugf("removing diffMessage data buffer. bytesRead exceeds %d bytes", BodySizeSmallEnoughToDiff)
		cff.diffMessageBuf.Close()
		cff.diffMessageBuf = nil
		cff.sw.bodyAct = BodyTooBig
	}

	if e := buf.Close(); e != nil {
		log.Debugf("closing batch buffer: %s", e)
		return 0, fmt.Errorf("error closing buffer: %w", e)
	}

	if len(buf.Bytes()) == 0 {
		log.Debug("batch is empty")
		return 0, nil
	}

	var doc interface{}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		return 0, fmt.Errorf("error parsing JSON bytes: %w", err)
	}
	validationState := jsch.Validate(ctx, doc)

	// If in strict mode, fail if there were any errors.
	if st.Strict && len(*validationState.Errs) > 0 {
		log.Debugf("%s. found at least %d errors", ErrStrictMode, len(*validationState.Errs))
		return 0, fmt.Errorf("%w. found at least %d errors", ErrStrictMode, len(*validationState.Errs))
	}

	if cff.publisher != nil && cff.bodySize > 0 {
		go func() {
			completion := float64(cff.teeReader.BytesRead()) / float64(cff.bodySize)
			evtErr := cff.publisher.Publish(ctx, event.ETDatasetSaveProgress, event.DsSaveEvent{
				Username:   cff.ds.Peername,
				Name:       cff.ds.Name,
				Message:    "processing body file",
				Completion: completion,
			})
			if evtErr != nil {
				log.Debugw("ignored error while publishing save progress", "evtErr", evtErr)
			}
		}()
	}

	return len(*validationState.Errs), nil
}

// getDepth finds the deepest value in a given interface value
func getDepth(x interface{}) (depth int) {
	switch v := x.(type) {
	case map[string]interface{}:
		for _, el := range v {
			if d := getDepth(el); d > depth {
				depth = d
			}
		}
		return depth + 1
	case []interface{}:
		for _, el := range v {
			if d := getDepth(el); d > depth {
				depth = d
			}
		}
		return depth + 1
	default:
		return depth
	}
}
