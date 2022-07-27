package lib

import (
	"context"
	"testing"

	testcfg "github.com/affix-io/affix/config/test"
	"github.com/affix-io/affix/event"
	"github.com/affix-io/affix/p2p"
	testrepo "github.com/affix-io/affix/repo/test"
)

func TestChanges(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	mr, err := testrepo.NewTestRepo()
	if err != nil {
		t.Fatalf("error allocating test repo: %s", err.Error())
	}
	node, err := p2p.NewaffixNode(mr, testcfg.DefaultP2PForTesting(), event.NilBus, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	inst := NewInstanceFromConfigAndNode(ctx, testcfg.DefaultConfigForTesting(), node)

	InitWorldBankDataset(ctx, t, inst)
	commitref := Commit2WorldBank(ctx, t, inst)

	// test ChangeReport with one param
	p := &ChangeReportParams{
		RightRef: commitref.Alias(),
	}
	if _, err := inst.Diff().Changes(ctx, p); err != nil {
		t.Fatalf("change report error: %s", err)
	}
}
