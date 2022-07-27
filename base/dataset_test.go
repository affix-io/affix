package base

import (
	"context"
	"testing"
	"time"

	"github.com/affix-io/affix/base/dsfs"
	"github.com/affix-io/affix/collection"
	"github.com/affix-io/affix/dsref"
	"github.com/affix-io/dataset/dstest"
	"github.com/google/go-cmp/cmp"
)

func TestListDatasets(t *testing.T) {
	ctx := context.Background()
	r := newTestRepo(t)
	ref := addCitiesDataset(t, r)

	// Limit to one
	res, err := ListDatasets(ctx, r, "", "", 0, 1, false, false)
	if err != nil {
		t.Error(err.Error())
	}
	if len(res) != 1 {
		t.Error("expected one dataset response")
	}

	// Limit to published datasets
	res, err = ListDatasets(ctx, r, "", "", 0, 1, true, false)
	if err != nil {
		t.Error(err.Error())
	}

	if len(res) != 0 {
		t.Error("expected no published datasets")
	}

	if err := SetPublishStatus(ctx, r, r.Profiles().Owner(ctx), ref, true); err != nil {
		t.Fatal(err)
	}

	// Limit to published datasets, after publishing cities
	res, err = ListDatasets(ctx, r, "", "", 0, 1, true, false)
	if err != nil {
		t.Error(err.Error())
	}

	if len(res) != 1 {
		t.Error("expected one published dataset response")
	}

	// Limit to datasets with "city" in their name
	res, err = ListDatasets(ctx, r, "city", "", 0, 1, false, false)
	if err != nil {
		t.Error(err.Error())
	}
	if len(res) != 0 {
		t.Error(`expected no datasets with "city" in their name`)
	}

	// Limit to datasets with "cit" in their name
	res, err = ListDatasets(ctx, r, "cit", "", 0, 1, false, false)
	if err != nil {
		t.Error(err.Error())
	}
	if len(res) != 1 {
		t.Error(`expected one dataset with \"cit\" in their name`)
	}

	res, err = ListDatasets(ctx, r, "", "", 0, -1, false, false)
	if err != nil {
		t.Error(err)
	}
	if len(res) != 1 {
		t.Errorf("expected -1 limit to return all datasets. got %d", len(res))
	}
}

func TestRawDatasetRefs(t *testing.T) {
	// to keep hashes consistent, artificially specify the timestamp by overriding
	// the dsfs.Timestamp func
	prev := dsfs.Timestamp
	defer func() { dsfs.Timestamp = prev }()
	minute := 0
	dsfs.Timestamp = func() time.Time {
		minute++
		return time.Date(2001, 01, 01, 01, minute, 01, 01, time.UTC)
	}

	ctx := context.Background()
	r := newTestRepo(t)
	s, err := collection.NewLocalSet(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	ref := addCitiesDataset(t, r)
	vi := dsref.NewVersionInfoFromRef(ref)
	vi.InitID = "AnInitID"
	if err := s.Add(ctx, r.Profiles().Active(ctx).ID, vi); err != nil {
		t.Fatal(err)
	}

	actual, err := RawDatasetRefs(ctx, r.Profiles().Active(ctx).ID, s)
	if err != nil {
		t.Fatal(err)
	}

	expect := dstest.Template(t, `0 Peername:  peer
  ProfileID: {{ .ProfileID }}
  Name:      cities
  Path:      {{ .Path }}
  Published: false
`, map[string]string{
		"ProfileID": "QmZePf5LeXow3RW5U1AgEiNbW46YnRGhZ7HPvm1UmPFPwt",
		"Path":      "/mem/QmbZEoPWbvtDhiLgEcteeBumC2sKQU1eVBEEvrRexexRMW",
	})

	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}
