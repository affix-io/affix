package dsfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	testkeys "github.com/affix-io/affix/auth/key/test"
	"github.com/affix-io/affix/base/toqtype"
	"github.com/affix-io/affix/event"
	"github.com/affix-io/dataset"
	"github.com/affix-io/dataset/dsio"
	"github.com/affix-io/dataset/dstest"
	"github.com/affix-io/dataset/generate"
	"github.com/affix-io/dataset/tabular"
	"github.com/affix-io/dataset/validate"
	"github.com/affix-io/qfs"
	"github.com/google/go-cmp/cmp"
)

func TestCreateDataset(t *testing.T) {
	ctx := context.Background()
	fs := qfs.NewMemFS()
	prev := Timestamp
	// shameless call to timestamp to get the coverge points
	Timestamp()
	defer func() { Timestamp = prev }()
	Timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	// These tests are using hard-coded ids that require this exact peer's private key.
	privKey := testkeys.GetKeyData(10).PrivKey

	bad := []struct {
		casePath   string
		resultPath string
		prev       *dataset.Dataset
		err        string
	}{
		{"invalid_reference",
			"", nil, "loading dataset commit: loading commit file: path not found"},
		{"invalid",
			"", nil, "commit is required"},
		{"strict_fail",
			"", nil, "processing body data: dataset body did not validate against schema in strict-mode. found at least 16 errors"},

		// // should error when previous dataset won't dereference.
		// {"craigslist",
		// 	"", &dataset.Dataset{Structure: dataset.NewStructureRef("/bad/path")}, 21, "error loading dataset structure: error loading structure file: cafs: path not found"},
		// // should error when previous dataset isn't valid. Aka, when it isn't empty, but missing
		// // either structure or commit. Commit is checked for first.
		// {"craigslist",
		// 	"", &dataset.Dataset{Meta: &dataset.Meta{Title: "previous"}, Structure: nil}, 21, "commit is required"},
	}

	for _, c := range bad {
		t.Run(fmt.Sprintf("bad_%s", c.casePath), func(t *testing.T) {
			tc, err := dstest.NewTestCaseFromDir("testdata/" + c.casePath)
			if err != nil {
				t.Fatalf("creating test case: %s", err)
			}

			_, err = CreateDataset(ctx, fs, fs, event.NilBus, tc.Input, c.prev, privKey, SaveSwitches{ShouldRender: true})
			if err == nil {
				t.Fatalf("CreateDataset expected error. got nil")
			}
			if err.Error() != c.err {
				t.Errorf("error string mismatch.\nwant: %q\ngot:  %q", c.err, err)
			}
		})
	}

	good := []struct {
		casePath   string
		resultPath string
		prev       *dataset.Dataset
		repoFiles  int // expected total count of files in repo after test execution
	}{
		{"cities",
			"/mem/QmcDaRWnD4e58HsM9rsT3SY5vfhK9hAqmFVppc71JnBEpi", nil, 8},
		{"all_fields",
			"/mem/QmQ2yM2pCQbYcWxdP4R1yeVKBkkMR8ZjKr3x8RzJfrXQmu", nil, 18},
		{"cities_no_commit_title",
			"/mem/QmVFBZpQ9k5w8jF9A1jTRfQ2YW5y4haSNjmqj5H9c23DqW", nil, 21},
		{"craigslist",
			"/mem/QmXhRb415KTb3zxGDwk3iehZ8S8BFzsEM3YiPgkPQr6VKf", nil, 27},
	}

	for _, c := range good {
		t.Run(fmt.Sprintf("good_%s", c.casePath), func(t *testing.T) {
			tc, err := dstest.NewTestCaseFromDir("testdata/" + c.casePath)
			if err != nil {
				t.Fatalf("creating test case: %s", err)
			}

			path, err := CreateDataset(ctx, fs, fs, event.NilBus, tc.Input, c.prev, privKey, SaveSwitches{ShouldRender: true})
			if err != nil {
				t.Fatalf("CreateDataset: %s", err)
			}

			ds, err := LoadDataset(ctx, fs, path)
			if err != nil {
				t.Fatalf("loading dataset: %s", err.Error())
			}
			ds.Path = ""

			if tc.Expect != nil {
				if diff := dstest.CompareDatasets(tc.Expect, ds); diff != "" {
					t.Errorf("dataset comparison error (-want +got): %s", diff)
					dstest.UpdateGoldenFileIfEnvVarSet(fmt.Sprintf("testdata/%s/expect.dataset.json", c.casePath), ds)
				}
			}

			if c.resultPath != path {
				t.Errorf("result path mismatch: expected: %q, got: %q", c.resultPath, path)
			}
			if c.repoFiles != len(fs.Files) {
				t.Errorf("invalid number of mapstore entries. want %d, got %d", c.repoFiles, len(fs.Files))
				return
			}
		})
	}

	t.Run("no_priv_key", func(t *testing.T) {
		_, err := CreateDataset(ctx, fs, fs, event.NilBus, nil, nil, nil, SaveSwitches{ShouldRender: true})
		if err == nil {
			t.Fatal("expected call without prvate key to error")
		}
		pkReqErrMsg := "private key is required to create a dataset"
		if err.Error() != pkReqErrMsg {
			t.Fatalf("error mismatch.\nwant: %q\ngot:  %q", pkReqErrMsg, err.Error())
		}
	})

	t.Run("no_body", func(t *testing.T) {
		dsData, err := ioutil.ReadFile("testdata/cities/input.dataset.json")
		if err != nil {
			t.Errorf("case nil body and previous body files, error reading dataset file: %s", err.Error())
		}
		ds := &dataset.Dataset{}
		if err := ds.UnmarshalJSON(dsData); err != nil {
			t.Errorf("case nil body and previous body files, error unmarshaling dataset file: %s", err.Error())
		}

		if err != nil {
			t.Errorf("case nil body and previous body files, error reading data file: %s", err.Error())
		}
		// expectedErr := "bodyfile or previous bodyfile needed"
		// _, err = CreateDataset(ctx, fs, fs, event.NilBus, ds, nil, privKey, SaveSwitches{ShouldRender: true})
		// if err.Error() != expectedErr {
		// 	t.Errorf("case nil body and previous body files, error mismatch: expected '%s', got '%s'", expectedErr, err.Error())
		// }
	})

	t.Run("no_changes", func(t *testing.T) {
		expectedErr := "saving failed: no changes"
		dsPrev, err := LoadDataset(ctx, fs, good[2].resultPath)
		if err != nil {
			t.Fatal(err)
		}

		ds := &dataset.Dataset{
			Name:      "cities",
			Commit:    &dataset.Commit{},
			Structure: dsPrev.Structure,
			Meta:      dsPrev.Meta,
		}
		ds.PreviousPath = good[2].resultPath
		if err != nil {
			t.Fatalf("loading previous dataset file: %s", err.Error())
		}

		bodyBytes, err := ioutil.ReadFile("testdata/cities/body.csv")
		if err != nil {
			t.Fatalf("reading body file: %s", err.Error())
		}
		ds.SetBodyFile(qfs.NewMemfileBytes("body.csv", bodyBytes))

		path, err := CreateDataset(ctx, fs, fs, event.NilBus, ds, dsPrev, privKey, SaveSwitches{ShouldRender: true})
		if err != nil && err.Error() != expectedErr {
			t.Fatalf("mismatch: expected %q, got %q", expectedErr, err.Error())
		} else if err == nil {
			ds, err := LoadDataset(ctx, fs, path)
			if err != nil {
				t.Fatalf("loading dataset: %s", err.Error())
			}

			t.Fatalf("CreateDataset expected error got 'nil'. commit: %v", ds.Commit)
		}

		if len(fs.Files) != 27 {
			t.Errorf("invalid number of entries. want %d got %d", 27, len(fs.Files))
			_, err := fs.Print()
			if err != nil {
				panic(err)
			}
		}
	})

	// case: previous dataset isn't valid
}

func TestDatasetSaveCustomTimestamp(t *testing.T) {
	ctx := context.Background()
	fs := qfs.NewMemFS()
	privKey := testkeys.GetKeyData(10).PrivKey

	// use a custom timestamp in local zone. should be converted to UTC for saving
	ts := time.Date(2100, 1, 2, 3, 4, 5, 6, time.Local)

	ds := &dataset.Dataset{
		Commit: &dataset.Commit{
			Timestamp: ts,
		},
		Structure: &dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray},
	}
	ds.SetBodyFile(qfs.NewMemfileBytes("/body.json", []byte(`[]`)))

	path, err := CreateDataset(ctx, fs, fs, event.NilBus, ds, nil, privKey, SaveSwitches{})
	if err != nil {
		t.Fatal(err)
	}

	got, err := LoadDataset(ctx, fs, path)
	if err != nil {
		t.Fatal(err)
	}

	if !ts.In(time.UTC).Equal(got.Commit.Timestamp) {
		t.Errorf("result timestamp mismatch.\nwant: %q\ngot:  %q", ts.In(time.UTC), got.Commit.Timestamp)
	}
}

func TestDatasetSaveEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs := qfs.NewMemFS()
	privKey := testkeys.GetKeyData(10).PrivKey
	bus := event.NewBus(ctx)

	fired := map[event.Type]int{}
	bus.SubscribeTypes(func(ctx context.Context, e event.Event) error {
		fired[e.Type]++
		return nil
	},
		event.ETDatasetSaveStarted,
		event.ETDatasetSaveProgress,
		event.ETDatasetSaveCompleted,
	)

	ds := &dataset.Dataset{
		Commit: &dataset.Commit{
			Timestamp: time.Date(2100, 1, 2, 3, 4, 5, 6, time.Local),
		},
		Structure: &dataset.Structure{Format: "json", Schema: dataset.BaseSchemaArray},
	}
	ds.SetBodyFile(qfs.NewMemfileBytes("/body.json", []byte(`[]`)))

	if _, err := CreateDataset(ctx, fs, fs, bus, ds, nil, privKey, SaveSwitches{}); err != nil {
		t.Fatal(err)
	}

	expect := map[event.Type]int{
		event.ETDatasetSaveStarted:   1,
		event.ETDatasetSaveProgress:  3,
		event.ETDatasetSaveCompleted: 1,
	}

	if diff := cmp.Diff(expect, fired); diff != "" {
		t.Errorf("fired event count mismatch. (-want +got):%s\n", diff)
	}
}

// Test that if the body is too large, the commit message just assumes the body changed
func TestCreateDatasetBodyTooLarge(t *testing.T) {
	ctx := context.Background()
	fs := qfs.NewMemFS()

	prevTs := Timestamp
	defer func() { Timestamp = prevTs }()
	Timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	// Set the limit for the body to be 100 bytes
	prevBodySizeLimit := BodySizeSmallEnoughToDiff
	defer func() { BodySizeSmallEnoughToDiff = prevBodySizeLimit }()
	BodySizeSmallEnoughToDiff = 100

	privKey := testkeys.GetKeyData(10).PrivKey

	// Need a previous commit, otherwise we just get the "created dataset" message
	prevDs := dataset.Dataset{
		Commit: &dataset.Commit{},
		Structure: &dataset.Structure{
			Format: "csv",
			Schema: tabular.BaseTabularSchema,
		},
	}

	testBodyPath, _ := filepath.Abs("testdata/movies/body.csv")
	testBodyBytes, _ := ioutil.ReadFile(testBodyPath)

	// Create a new version and add the body
	nextDs := dataset.Dataset{
		Commit: &dataset.Commit{},
		Structure: &dataset.Structure{
			Format: "csv",
			Schema: tabular.BaseTabularSchema,
		},
	}
	nextDs.SetBodyFile(qfs.NewMemfileBytes(testBodyPath, testBodyBytes))

	path, err := CreateDataset(ctx, fs, fs, event.NilBus, &nextDs, &prevDs, privKey, SaveSwitches{ShouldRender: true})
	if err != nil {
		t.Fatalf("CreateDataset: %s", err)
	}

	// Load the created dataset to inspect the commit message
	got, err := LoadDataset(ctx, fs, path)
	if err != nil {
		t.Fatalf("LoadDataset: %s", err)
	}

	expect := dstest.LoadGoldenFile(t, "testdata/movies/expect.dataset.json")
	if diff := dstest.CompareDatasets(expect, got); diff != "" {
		t.Errorf("result mismatch (-want +got):%s\n", diff)
		dstest.UpdateGoldenFileIfEnvVarSet("testdata/movies/expect.dataset.json", got)
	}
}

func TestWriteDataset(t *testing.T) {
	ctx := context.Background()
	fs := qfs.NewMemFS()
	prev := Timestamp
	defer func() { Timestamp = prev }()
	Timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	// These tests are using hard-coded ids that require this exact peer's private key.
	pk := testkeys.GetKeyData(10).PrivKey

	if _, err := WriteDataset(ctx, fs, fs, nil, &dataset.Dataset{}, event.NilBus, pk, SaveSwitches{Pin: true}); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}

	cases := []struct {
		casePath  string
		repoFiles int // expected total count of files in repo after test execution
		err       string
	}{
		// TODO (b5) - these are *very* close, need to be fixed
		// {"cities", 6, ""},      // dataset, commit, structure, meta, viz, body
		// {"all_fields", 14, ""}, // dataset, commit, structure, meta, viz, viz_script, transform, transform_script, SAME BODY as cities -> gets de-duped
	}

	for i, c := range cases {
		tc, err := dstest.NewTestCaseFromDir("testdata/" + c.casePath)
		if err != nil {
			t.Errorf("%s: error creating test case: %s", c.casePath, err)
			continue
		}

		ds := tc.Input

		got, err := WriteDataset(ctx, fs, fs, nil, ds, event.NilBus, pk, SaveSwitches{Pin: true})
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		// total count expected of files in repo after test execution
		if len(fs.Files) != c.repoFiles {
			t.Errorf("case expected %d invalid number of entries: %d != %d", i, c.repoFiles, len(fs.Files))
			str, err := fs.Print()
			if err != nil {
				panic(err)
			}
			t.Log(str)
			continue
		}

		got = PackageFilepath(fs, got, PackageFileDataset)

		f, err := fs.Get(ctx, got)
		if err != nil {
			t.Errorf("error getting dataset file: %s", err.Error())
			continue
		}

		ref := &dataset.Dataset{}
		if err := json.NewDecoder(f).Decode(ref); err != nil {
			t.Errorf("error decoding dataset json: %s", err.Error())
			continue
		}

		if ref.Transform != nil {
			if ref.Transform.IsEmpty() {
				t.Errorf("expected stored dataset.Transform to be populated")
			}
			ds.Transform.Assign(dataset.NewTransformRef(ref.Transform.Path))
		}
		if ref.Meta != nil {
			if !ref.Meta.IsEmpty() {
				t.Errorf("expected stored dataset.Meta to be a reference")
			}
			// Abstract transforms aren't loaded
			ds.Meta.Assign(dataset.NewMetaRef(ref.Meta.Path))
		}
		if ref.Structure != nil {
			if !ref.Structure.IsEmpty() {
				t.Errorf("expected stored dataset.Structure to be a reference")
			}
			ds.Structure.Assign(dataset.NewStructureRef(ref.Structure.Path))
		}
		if ref.Viz != nil {
			if ref.Viz.IsEmpty() {
				t.Errorf("expected stored dataset.Viz to be populated")
			}
			ds.Viz.Assign(dataset.NewVizRef(ref.Viz.Path))
		}
		ds.BodyPath = ref.BodyPath

		ds.Assign(dataset.NewDatasetRef(got))
		result, err := LoadDataset(ctx, fs, got)
		if err != nil {
			t.Errorf("case %d unexpected error loading dataset: %s", i, err)
			continue
		}

		if diff := dstest.CompareDatasets(ds, result); diff != "" {
			t.Errorf("case %d comparison mismatch: (-want +got):\n%s", i, diff)

			d1, _ := ds.MarshalJSON()
			t.Log(string(d1))

			d, _ := result.MarshalJSON()
			t.Log(string(d))
			continue
		}
	}
}

func TestGenerateCommitMessage(t *testing.T) {
	badCases := []struct {
		description string
		prev, ds    *dataset.Dataset
		force       bool
		errMsg      string
	}{
		{
			"no changes from one dataset version to next",
			&dataset.Dataset{Meta: &dataset.Meta{Title: "same dataset"}},
			&dataset.Dataset{Meta: &dataset.Meta{Title: "same dataset"}},
			false,
			"no changes",
		},
	}

	ctx := context.Background()
	fs := qfs.NewMemFS()

	for _, c := range badCases {
		t.Run(fmt.Sprintf("%s", c.description), func(t *testing.T) {
			_, _, err := generateCommitDescriptions(ctx, fs, c.ds, c.prev, BodySame, c.force)
			if err == nil {
				t.Errorf("error expected, did not get one")
			} else if c.errMsg != err.Error() {
				t.Errorf("error mismatch\nexpect: %s\ngot: %s", c.errMsg, err.Error())
			}
		})
	}

	goodCases := []struct {
		description string
		prev, ds    *dataset.Dataset
		force       bool
		expectShort string
		expectLong  string
	}{
		{
			"empty previous and non-empty dataset",
			&dataset.Dataset{},
			&dataset.Dataset{Meta: &dataset.Meta{Title: "new dataset"}},
			false,
			"created dataset",
			"created dataset",
		},
		{
			"title changes from previous",
			&dataset.Dataset{Meta: &dataset.Meta{Title: "new dataset"}},
			&dataset.Dataset{Meta: &dataset.Meta{Title: "changes to dataset"}},
			false,
			"meta updated title",
			"meta:\n\tupdated title",
		},
		{
			"same dataset but force is true",
			&dataset.Dataset{Meta: &dataset.Meta{Title: "same dataset"}},
			&dataset.Dataset{Meta: &dataset.Meta{Title: "same dataset"}},
			true,
			"forced update",
			"forced update",
		},
		{
			"structure sets the headerRow config option",
			&dataset.Dataset{Structure: &dataset.Structure{
				FormatConfig: map[string]interface{}{
					"headerRow": false,
				},
			}},
			&dataset.Dataset{Structure: &dataset.Structure{
				FormatConfig: map[string]interface{}{
					"headerRow": true,
				},
			}},
			false,
			"structure updated formatConfig.headerRow",
			"structure:\n\tupdated formatConfig.headerRow",
		},
		{
			"readme modified",
			&dataset.Dataset{Readme: &dataset.Readme{
				Format: "md",
				Text:   "# hello\n\ncontent\n\n",
			}},
			&dataset.Dataset{Readme: &dataset.Readme{
				Format: "md",
				Text:   "# hello\n\ncontent\n\nanother line\n\n",
			}},
			false,
			// TODO(dustmop): Should mention the line added.
			"readme updated text",
			"readme:\n\tupdated text",
		},
		{
			"body with a small number of changes",
			&dataset.Dataset{
				Structure: &dataset.Structure{Format: "json"},
				Body: toqtype.MustParseJSONAsArray(`[
  { "fruit": "apple", "color": "red" },
  { "fruit": "banana", "color": "yellow" },
  { "fruit": "cherry", "color": "red" }
]`),
			},
			&dataset.Dataset{
				Structure: &dataset.Structure{Format: "json"},
				Body: toqtype.MustParseJSONAsArray(`[
  { "fruit": "apple", "color": "red" },
  { "fruit": "blueberry", "color": "blue" },
  { "fruit": "cherry", "color": "red" },
  { "fruit": "durian", "color": "green" }
]`),
			},
			false,
			"body updated row 1 and added row 3",
			"body:\n\tupdated row 1\n\tadded row 3",
		},
		{
			"body with lots of changes",
			&dataset.Dataset{
				Structure: &dataset.Structure{Format: "csv"},
				Body: toqtype.MustParseCsvAsArray(`one,two,3
four,five,6
seven,eight,9
ten,eleven,12
thirteen,fourteen,15
sixteen,seventeen,18
nineteen,twenty,21
twenty-two,twenty-three,24
twenty-five,twenty-six,27
twenty-eight,twenty-nine,30`),
			},
			&dataset.Dataset{
				Structure: &dataset.Structure{Format: "csv"},
				Body: toqtype.MustParseCsvAsArray(`one,two,3
four,five,6
seven,eight,cat
dog,eleven,12
thirteen,eel,15
sixteen,seventeen,100
frog,twenty,21
twenty-two,twenty-three,24
twenty-five,giraffe,200
hen,twenty-nine,30`),
			},
			false,
			"body changed by 19%",
			"body:\n\tchanged by 19%",
		},
		{
			"meta and structure and readme changes",
			&dataset.Dataset{
				Meta: &dataset.Meta{Title: "new dataset"},
				Structure: &dataset.Structure{
					FormatConfig: map[string]interface{}{
						"headerRow": false,
					},
				},
				Readme: &dataset.Readme{
					Format: "md",
					Text:   "# hello\n\ncontent\n\n",
				},
			},
			&dataset.Dataset{
				Meta: &dataset.Meta{Title: "changes to dataset"},
				Structure: &dataset.Structure{
					FormatConfig: map[string]interface{}{
						"headerRow": true,
					},
				},
				Readme: &dataset.Readme{
					Format: "md",
					Text:   "# hello\n\ncontent\n\nanother line\n\n",
				},
			},
			false,
			"updated meta, structure, and readme",
			"meta:\n\tupdated title\nstructure:\n\tupdated formatConfig.headerRow\nreadme:\n\tupdated text",
		},
		{
			"meta removed but everything else is the same",
			&dataset.Dataset{
				Meta: &dataset.Meta{Title: "new dataset"},
				Structure: &dataset.Structure{
					FormatConfig: map[string]interface{}{
						"headerRow": false,
					},
				},
				Readme: &dataset.Readme{
					Format: "md",
					Text:   "# hello\n\ncontent\n\n",
				},
			},
			&dataset.Dataset{
				Structure: &dataset.Structure{
					FormatConfig: map[string]interface{}{
						"headerRow": false,
					},
				},
				Readme: &dataset.Readme{
					Format: "md",
					Text:   "# hello\n\ncontent\n\n",
				},
			},
			false,
			"meta removed",
			"meta removed",
		},
		{
			"meta has multiple parts changed",
			&dataset.Dataset{
				Meta: &dataset.Meta{
					Title:       "new dataset",
					Description: "TODO: Add description",
				},
			},
			&dataset.Dataset{
				Meta: &dataset.Meta{
					Title:       "changes to dataset",
					HomeURL:     "http://example.com",
					Description: "this is a great description",
				},
			},
			false,
			"meta updated 3 fields",
			"meta:\n\tupdated description\n\tadded homeURL\n\tupdated title",
		},
		{
			"meta and body changed",
			&dataset.Dataset{
				Meta: &dataset.Meta{
					Title:       "new dataset",
					Description: "TODO: Add description",
				},
				Structure: &dataset.Structure{Format: "csv"},
				Body: toqtype.MustParseCsvAsArray(`one,two,3
four,five,6
seven,eight,9
ten,eleven,12
thirteen,fourteen,15
sixteen,seventeen,18
nineteen,twenty,21
twenty-two,twenty-three,24
twenty-five,twenty-six,27
twenty-eight,twenty-nine,30`),
			},
			&dataset.Dataset{
				Meta: &dataset.Meta{
					Title:       "changes to dataset",
					HomeURL:     "http://example.com",
					Description: "this is a great description",
				},
				Structure: &dataset.Structure{Format: "csv"},
				Body: toqtype.MustParseCsvAsArray(`one,two,3
four,five,6
something,eight,cat
dog,eleven,12
thirteen,eel,15
sixteen,60,100
frog,twenty,21
twenty-two,twenty-three,24
twenty-five,giraffe,200
hen,twenty-nine,30`),
			},
			false,
			"updated meta and body",
			"meta:\n\tupdated description\n\tadded homeURL\n\tupdated title\nbody:\n\tchanged by 24%",
		},
		{
			"meta changed but body stays the same",
			&dataset.Dataset{
				Meta: &dataset.Meta{
					Title: "new dataset",
				},
				Structure: &dataset.Structure{Format: "csv"},
				Body: toqtype.MustParseCsvAsArray(`one,two,3
four,five,6
seven,eight,9
ten,eleven,12
thirteen,fourteen,15
sixteen,seventeen,18`),
			},
			&dataset.Dataset{
				Meta: &dataset.Meta{
					Title: "dataset of a bunch of numbers",
				},
				Structure: &dataset.Structure{Format: "csv"},
				Body: toqtype.MustParseCsvAsArray(`one,two,3
four,five,6
seven,eight,9
ten,eleven,12
thirteen,fourteen,15
sixteen,seventeen,18`),
			},
			false,
			"meta updated title",
			"meta:\n\tupdated title",
		},
	}

	for _, c := range goodCases {
		t.Run(c.description, func(t *testing.T) {
			bodyAct := BodyDefault
			if compareBody(c.prev.Body, c.ds.Body) {
				bodyAct = BodySame
			}
			shortTitle, longMessage, err := generateCommitDescriptions(ctx, fs, c.ds, c.prev, bodyAct, c.force)
			if err != nil {
				t.Errorf("error: %s", err.Error())
				return
			}
			if c.expectShort != shortTitle {
				t.Errorf("short message mismatch\nexpect: %s\ngot: %s", c.expectShort, shortTitle)
			}
			if c.expectLong != longMessage {
				t.Errorf("long message mismatch\nexpect: %s\ngot: %s", c.expectLong, longMessage)
			}
		})
	}
}

func compareBody(left, right interface{}) bool {
	leftData, err := json.Marshal(left)
	if err != nil {
		panic(err)
	}
	rightData, err := json.Marshal(right)
	if err != nil {
		panic(err)
	}
	return string(leftData) == string(rightData)
}

func TestGetDepth(t *testing.T) {
	good := []struct {
		val      string
		expected int
	}{
		{`"foo"`, 0},
		{`1000`, 0},
		{`true`, 0},
		{`{"foo": "bar"}`, 1},
		{`{"foo": "bar","bar": "baz"}`, 1},
		{`{
			"foo":"bar",
			"bar": "baz",
			"baz": {
				"foo": "bar",
				"bar": "baz"
			}
		}`, 2},
		{`{
			"foo": "bar",
			"bar": "baz",
			"baz": {
				"foo": "bar",
				"bar": [
					"foo",
					"bar",
					"baz"
				]
			}
		}`, 3},
		{`{
			"foo": "bar",
			"bar": "baz",
			"baz": [
				"foo",
				"bar",
				"baz"
			]
		}`, 2},
		{`["foo","bar","baz"]`, 1},
		{`["a","b",[1, 2, 3]]`, 2},
		{`[
			"foo",
			"bar",
			{"baz": {
				"foo": "bar",
				"bar": "baz",
				"baz": "foo"
				}
			}
		]`, 3},
		{`{
			"foo": "bar",
			"foo1": {
				"foo2": 2,
				"foo3": false
			},
			"foo4": "bar",
			"foo5": {
				"foo6": 100
			}
		}`, 2},
		{`{
			"foo":  "bar",
			"foo1": "bar",
			"foo2": {
				"foo3": 100,
				"foo4": 100
			},
			"foo5": {
				"foo6": 100,
				"foo7": 100,
				"foo8": 100,
				"foo9": 100
			},
			"foo10": {
				"foo11": 100,
				"foo12": 100
			}
		}`, 2},
	}

	var val interface{}

	for i, c := range good {
		if err := json.Unmarshal([]byte(c.val), &val); err != nil {
			t.Fatal(err)
		}
		depth := getDepth(val)
		if c.expected != depth {
			t.Errorf("case %d, depth mismatch, expected %d, got %d", i, c.expected, depth)
		}
	}
}

func GenerateDataset(b *testing.B, sampleSize int, format string) (int, *dataset.Dataset) {
	ds := &dataset.Dataset{
		Commit: &dataset.Commit{
			Timestamp: time.Date(2017, 1, 1, 1, 0, 0, 0, time.UTC),
			Title:     "initial commit",
		},
		Meta: &dataset.Meta{
			Title: "performance benchmark data",
		},
		Structure: &dataset.Structure{
			Format: format,
			FormatConfig: map[string]interface{}{
				"headerRow":  true,
				"lazyQuotes": true,
			},
			Schema: map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "array",
					"items": []interface{}{
						map[string]interface{}{"title": "uuid", "type": "string"},
						map[string]interface{}{"title": "ingest", "type": "string"},
						map[string]interface{}{"title": "occurred", "type": "string"},
						map[string]interface{}{"title": "raw_data", "type": "string"},
					},
				},
			},
		},
	}

	gen, err := generate.NewTabularGenerator(ds.Structure)
	if err != nil {
		b.Errorf("error creating generator: %s", err.Error())
	}
	defer gen.Close()

	bodyBuffer := &bytes.Buffer{}
	w, err := dsio.NewEntryWriter(ds.Structure, bodyBuffer)
	if err != nil {
		b.Fatalf("creating entry writer: %s", err.Error())
	}

	for i := 0; i < sampleSize; i++ {
		ent, err := gen.ReadEntry()
		if err != nil {
			b.Fatalf("reading generator entry: %s", err.Error())
		}
		w.WriteEntry(ent)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("closing writer: %s", err)
	}

	fileName := fmt.Sprintf("body.%s", ds.Structure.Format)
	ds.SetBodyFile(qfs.NewMemfileReader(fileName, bodyBuffer))

	return bodyBuffer.Len(), ds
}

func BenchmarkCreateDatasetCSV(b *testing.B) {
	// ~1 MB, ~12 MB, ~25 MB, ~50 MB, ~500 MB, ~1GB
	for _, sampleSize := range []int{10000, 100000, 250000, 500000, 1000000} {
		ctx := context.Background()
		fs := qfs.NewMemFS()
		prev := Timestamp

		defer func() { Timestamp = prev }()
		Timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

		// These tests are using hard-coded ids that require this exact peer's private key.
		privKey := testkeys.GetKeyData(10).PrivKey

		b.Run(fmt.Sprintf("sample size %v", sampleSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				_, dataset := GenerateDataset(b, sampleSize, "csv")

				b.StartTimer()
				_, err := CreateDataset(ctx, fs, fs, event.NilBus, dataset, nil, privKey, SaveSwitches{ShouldRender: true})
				if err != nil {
					b.Errorf("error creating dataset: %s", err.Error())
				}
			}
			b.StopTimer()
		})
	}
}

// validateDataset is a stripped copy of base/dsfs/setErrCount
func validateDataset(ds *dataset.Dataset, data qfs.File) error {
	defer data.Close()

	er, err := dsio.NewEntryReader(ds.Structure, data)
	if err != nil {
		return err
	}

	_, err = validate.EntryReader(er)

	return err
}

func BenchmarkValidateCSV(b *testing.B) {
	// ~1 MB, ~12 MB, ~25 MB, ~50 MB, ~500 MB, ~1GB
	for _, sampleSize := range []int{10000, 100000, 250000, 500000, 1000000, 10000000} {
		b.Run(fmt.Sprintf("sample size %v", sampleSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				_, dataset := GenerateDataset(b, sampleSize, "csv")

				b.StartTimer()
				err := validateDataset(dataset, dataset.BodyFile())
				if err != nil {
					b.Errorf("error creating dataset: %s", err.Error())
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkValidateJSON(b *testing.B) {
	// ~1 MB, ~12 MB, ~25 MB, ~50 MB, ~500 MB, ~1GB
	for _, sampleSize := range []int{10000, 100000, 250000, 500000, 1000000, 10000000} {
		b.Run(fmt.Sprintf("sample size %v", sampleSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				_, dataset := GenerateDataset(b, sampleSize, "json")

				b.StartTimer()
				err := validateDataset(dataset, dataset.BodyFile())
				if err != nil {
					b.Errorf("error creating dataset: %s", err.Error())
				}
			}
			b.StopTimer()
		})
	}
}
