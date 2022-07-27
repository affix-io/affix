package collection

import (
	"context"
	"testing"
	"time"

	"github.com/affix-io/affix/automation/run"
	"github.com/affix-io/affix/base/params"
	"github.com/affix-io/affix/dsref"
	"github.com/affix-io/affix/repo"
	repotest "github.com/affix-io/affix/repo/test"
	"github.com/affix-io/dataset"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestMigrateRepoStoreToLocalCollectionSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := repotest.NewTestRepo()
	if err != nil {
		t.Fatal(err)
	}

	expect, err := repo.ListVersionInfoShim(r, 0, 100000)
	if err != nil {
		t.Error(err)
	}

	if len(expect) == 0 {
		t.Fatalf("test repo has no datasets")
	}

	// force log entries for runs
	book := r.Logbook()
	pro := r.Profiles().Owner(ctx)
	citiesRef := &dsref.Ref{Username: "peer", Name: "cities"}
	_, err = r.ResolveRef(ctx, citiesRef)
	if err != nil {
		t.Fatalf("test repo cannot resolve dataset ref %q", "peer/cities")
	}
	err = book.WriteTransformRun(ctx, pro, citiesRef.InitID, &run.State{ID: "cities_run_id", Status: run.RSSucceeded, Duration: 1000})
	if err != nil {
		t.Fatalf("unable to add transform run op to logbook for dataset %s, %q", "peer/cities", err)
	}

	expect[0].RunCount = 1
	expect[0].RunID = "cities_run_id"
	expect[0].RunStatus = "succeeded"
	expect[0].RunDuration = 1000

	citiesDS := &dataset.Dataset{ID: citiesRef.InitID, ProfileID: citiesRef.ProfileID, Peername: citiesRef.Username, Name: citiesRef.Name, PreviousPath: citiesRef.Path, Path: "new_path", Commit: &dataset.Commit{Timestamp: time.Unix(10000, 0), Title: "delete this commit"}}
	if err = book.WriteVersionSave(ctx, pro, citiesDS, nil); err != nil {
		t.Fatalf("unable to add commit op to logbook for dataset %s, %q", "peer/cities", err)
	}
	if err = book.WriteVersionDelete(ctx, pro, citiesRef.InitID, 1); err != nil {
		t.Fatalf("unable to add delete op to logbook for dataset %s, %q", "peer/cities", err)
	}

	for i := 0; i < len(expect); i++ {
		expect[i].CommitCount = 1
	}

	// migrate
	set, err := NewLocalSet(ctx, "", func(o *LocalSetOptions) {
		o.MigrateRepo = r
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := set.List(ctx, r.Profiles().Owner(ctx).ID, params.ListAll)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(expect, got, cmpopts.IgnoreFields(dsref.VersionInfo{}, "InitID", "MetaTitle", "ThemeList", "BodySize", "BodyRows", "CommitTime", "NumErrors", "CommitTitle")); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}
