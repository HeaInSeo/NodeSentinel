package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// TestPlanStages_RequestedActionsContract pins the requested_actions ->
// optional-stage contract (docs/NODESENTINEL_VALIDATION_FLOW_SPEC_v0.1.md
// §4.3) directly on planStages: empty means the legacy run-everything plan,
// smoke_run alone selects no L5 stage, profile selects only L5-a,
// security_scan selects only L5-b, order and duplicates don't matter, and any
// unknown value is a deterministic error with no partial plan.
func TestPlanStages_RequestedActionsContract(t *testing.T) {
	tests := []struct {
		name      string
		requested []work.Action
		want      stagePlan
		wantErr   bool
	}{
		{name: "nil is legacy all stages", requested: nil, want: stagePlan{runL5a: true, runL5b: true}},
		{name: "empty is legacy all stages", requested: []work.Action{}, want: stagePlan{runL5a: true, runL5b: true}},
		{name: "smoke_run only", requested: []work.Action{work.ActionSmokeRun}, want: stagePlan{}},
		{name: "smoke_run+profile", requested: []work.Action{work.ActionSmokeRun, work.ActionProfile}, want: stagePlan{runL5a: true}},
		{name: "smoke_run+security_scan", requested: []work.Action{work.ActionSmokeRun, work.ActionSecurityScan}, want: stagePlan{runL5b: true}},
		{name: "all three", requested: []work.Action{work.ActionSmokeRun, work.ActionProfile, work.ActionSecurityScan}, want: stagePlan{runL5a: true, runL5b: true}},
		{name: "order independent", requested: []work.Action{work.ActionSecurityScan, work.ActionProfile, work.ActionSmokeRun}, want: stagePlan{runL5a: true, runL5b: true}},
		{name: "duplicates", requested: []work.Action{work.ActionProfile, work.ActionSmokeRun, work.ActionProfile}, want: stagePlan{runL5a: true}},
		{name: "bogus only", requested: []work.Action{"bogus"}, wantErr: true},
		{name: "bogus after valid", requested: []work.Action{work.ActionSmokeRun, work.ActionProfile, "bogus"}, wantErr: true},
		{name: "case sensitive", requested: []work.Action{work.ActionSmokeRun, "Profile"}, wantErr: true},
		{name: "blank entry", requested: []work.Action{work.ActionSmokeRun, ""}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := planStages(tt.requested)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("planStages(%v) = %+v, nil; want an error", tt.requested, got)
				}
				if got != (stagePlan{}) {
					t.Fatalf("planStages(%v) returned partial plan %+v with its error", tt.requested, got)
				}
				// Deterministic: the same input fails the same way every time.
				if _, again := planStages(tt.requested); again == nil || again.Error() != err.Error() {
					t.Fatalf("planStages(%v) error not deterministic: %v then %v", tt.requested, err, again)
				}
				return
			}
			if err != nil {
				t.Fatalf("planStages(%v): %v", tt.requested, err)
			}
			if got != tt.want {
				t.Fatalf("planStages(%v) = %+v, want %+v", tt.requested, got, tt.want)
			}
		})
	}
}

// TestProcess_SecurityScanOnlyRequested_L5bRunsL5aSkipped covers the
// smoke_run+security_scan selection: L5-b runs and carries the job's one
// Terminal record, while the skipped L5-a produces no check record at all —
// a skipped stage is never reported as an observed profile. With no scan
// infrastructure (no dynamic client) the L5-b record is "not-available",
// which must not read as a passed security observation (DQ-R2.1-N2).
func TestProcess_SecurityScanOnlyRequested_L5bRunsL5aSkipped(t *testing.T) {
	useFastWorkerTicks(t)

	var captured []capturedSubmission
	vc := capturingVaultServer(t, &captured)

	store := newTestStore(t)
	kube := fake.NewClientset()
	kube.PrependReactor("create", "jobs", absorbDryRunReactor())
	kube.PrependReactor("get", "jobs", alwaysCompleteReactor(smokeNamespace))

	w := New(store, kube, "test-worker").WithVaultClient(vc)

	req := newTestJob()
	req.RequestedActions = []work.Action{work.ActionSmokeRun, work.ActionSecurityScan}
	created, err := store.CreateJob(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := store.LeaseJob(context.Background(), "test-worker", 60*time.Second)
	if err != nil {
		t.Fatalf("LeaseJob: %v", err)
	}

	w.process(context.Background(), job)

	var scans []vaultclient.SubmitScanRecordRequest
	terminals := 0
	for _, c := range captured {
		switch {
		case strings.Contains(c.path, "scan-records"):
			s := c.decodeScan(t)
			scans = append(scans, s)
			if s.Terminal {
				terminals++
			}
		case strings.Contains(c.path, "check-records"):
			chk := c.decodeCheck(t)
			if chk.Stage == vaultclient.StageL5A {
				t.Errorf("L5-a check record submitted although profile was not requested: %+v", chk)
			}
			if chk.Terminal {
				terminals++
			}
		}
	}
	if len(scans) != 1 {
		t.Fatalf("scan records = %d, want exactly 1 (security_scan was requested)", len(scans))
	}
	scan := scans[0]
	if scan.Stage != vaultclient.StageL5B || !scan.Terminal {
		t.Errorf("scan record Stage=%q Terminal=%v, want L5B terminal — L5-b is the last stage this plan runs", scan.Stage, scan.Terminal)
	}
	if terminals != 1 {
		t.Errorf("Terminal records = %d, want exactly 1", terminals)
	}
	if scan.PolicyResult != "not-available" || scan.Source != "not-available" {
		t.Errorf("scan PolicyResult=%q Source=%q, want not-available/not-available without scan infrastructure",
			scan.PolicyResult, scan.Source)
	}
	if scan.PolicyResult == "passed" || scan.CriticalCount != 0 || scan.HighCount != 0 {
		t.Errorf("not-available scan record reads as an observed result: %+v", scan)
	}

	stored, err := store.GetJob(context.Background(), created.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stored.Status != work.StatusSucceeded {
		t.Errorf("expected status=succeeded, got %q", stored.Status)
	}
	if !stored.TerminalSubmitted {
		t.Error("expected TerminalSubmitted=true")
	}
	if !strings.Contains(stored.ResultSummary, "L5-a skipped (not requested)") {
		t.Errorf("ResultSummary should note L5-a was skipped, got %q", stored.ResultSummary)
	}
	if !strings.Contains(stored.ResultSummary, "L5-b submitted") {
		t.Errorf("ResultSummary should note L5-b was submitted, got %q", stored.ResultSummary)
	}
}
