package internal

import (
	"context"
	"encoding/base64"
	"errors"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
)

const (
	conflictNetworkFee      int64  = 2_000_000
	conflictValidUntilBlock uint32 = 1200
)

func generateConflictPairs(ctx context.Context, opts BenchOptions, callback ...GenerateCallback) *Dump {
	if len(opts.Senders) == 0 {
		log.Fatal("conflict scenario: no senders available")
	}
	if opts.TxCount%2 != 0 {
		log.Fatalf("conflict scenario: tx count must be even to form pairs, got %d", opts.TxCount)
	}

	pairsCount := int(opts.TxCount) / 2
	dump := Dump{TransactionsQueue: queue.NewRingBuffer(opts.TxCount)}

	log.Printf("Generating %d conflicting transaction pairs (%d transactions)", pairsCount, opts.TxCount)

	for i := range pairsCount {
		if ctx.Err() != nil {
			log.Fatal(ctx.Err())
		}

		sender := opts.Senders[i%len(opts.Senders)]
		txA, txB := newConflictingPair(i, sender)

		for _, tx := range []*transaction.Transaction{txA, txB} {
			blob := encodeTx(tx)

			if err := dump.TransactionsQueue.Put(blob); err != nil {
				log.Fatalf("cannot enqueue conflicting tx #%d: %s", i, err)
			}
			for j := range callback {
				if err := callback[j](tx.Hash().String(), blob); err != nil {
					log.Fatalf("callback returned error: %d %v", i, err)
				}
			}
		}
	}

	log.Printf("Done generating conflicting pairs")
	return &dump
}

func newConflictingPair(idx int, sender *keys.PrivateKey) (txA, txB *transaction.Transaction) {
	txA = newTx(sender, uint32(2*idx))

	hashA := txA.Hash()

	txB = newTx(sender, uint32(2*idx+1))
	txB.Attributes = append(txB.Attributes, transaction.Attribute{
		Type:  transaction.ConflictsT,
		Value: &transaction.Conflicts{Hash: hashA},
	})

	acc := wallet.NewAccountFromPrivateKey(sender)
	if err := acc.SignTx(netmode.PrivNet, txA); err != nil {
		log.Fatalf("could not sign conflicting tx A: %v", err)
	}
	if err := acc.SignTx(netmode.PrivNet, txB); err != nil {
		log.Fatalf("could not sign conflicting tx B: %v", err)
	}

	return txA, txB
}

func newTx(sender *keys.PrivateKey, nonce uint32) *transaction.Transaction {
	tx := transaction.New([]byte{byte(opcode.RET)}, 1_000_000)
	tx.Nonce = nonce
	tx.NetworkFee = conflictNetworkFee
	tx.ValidUntilBlock = conflictValidUntilBlock
	tx.Signers = []transaction.Signer{{
		Account: sender.GetScriptHash(),
		Scopes:  transaction.None,
	}}
	return tx
}

func encodeTx(tx *transaction.Transaction) string {
	buf := io.NewBufBinWriter()
	tx.EncodeBinary(buf.BinWriter)
	if buf.Err != nil {
		log.Fatalf("could not encode conflicting tx: %v", buf.Err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func pickRandomNodePair(n int) (int, int) {
	a := rand.IntN(n)
	b := rand.IntN(n)
	for b == a {
		b = rand.IntN(n)
	}
	return a, b
}

func (d *doer) SendConflictPairs(ctx context.Context) {
	defer close(d.sentOut)

	nodeCount := d.cli.AddrCount()
	start := time.Now()

	for range d.wrkCount {
		d.waiter.Go(func() {
			done := ctx.Done()
			timer := time.NewTimer(d.timeLimit)
			defer timer.Stop()

			for {
				select {
				case <-done:
					return
				case <-timer.C:
					return
				default:
				}

				blobA, blobB, ok := d.claimConflictPair()
				if !ok {
					return
				}

				nodeA, nodeB := pickRandomNodePair(nodeCount)

				var wgPair sync.WaitGroup
				wgPair.Add(2)
				go func() {
					defer wgPair.Done()
					d.sendConflictTx(ctx, blobA, nodeA, start)
				}()
				go func() {
					defer wgPair.Done()
					d.sendConflictTx(ctx, blobB, nodeB, start)
				}()
				wgPair.Wait()
			}
		})
	}
	d.waiter.Wait()

	d.reportSendResult(start)
}

func (d *doer) claimConflictPair() (string, string, bool) {
	d.Lock()
	defer d.Unlock()

	if d.dump.TransactionsQueue.Len() < 2 {
		return "", "", false
	}

	a, err := d.dump.TransactionsQueue.Get()
	if err != nil {
		return "", "", false
	}
	b, err := d.dump.TransactionsQueue.Get()
	if err != nil {
		return "", "", false
	}

	return a.(string), b.(string), true
}

func (d *doer) sendConflictTx(ctx context.Context, blob string, nodeIdx int, start time.Time) {
	err := d.cli.SendTXToNode(ctx, blob, nodeIdx)
	switch {
	case err == nil:
	case errors.Is(err, ErrConflict):
		return
	case errors.Is(err, ErrMempoolOOM):
		if putErr := d.dump.TransactionsQueue.Put(blob); putErr != nil {
			log.Printf("failed to re-enqueue conflicting tx: %s", putErr)
			d.countErr.Add(1)
		}
		time.Sleep(d.mempoolOOMDelay)
		return
	default:
		d.countErr.Add(1)
		return
	}

	count := d.countTxs.Add(1)
	d.rpsReporter(float64(count) / time.Since(start).Seconds())
}
