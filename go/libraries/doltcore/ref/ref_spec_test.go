package ref

import "testing"

func TestRefSpec(t *testing.T) {
	tests := []struct {
		remote     string
		refSpecStr string
		isValid    bool
		inToExpOut map[string]string
	}{
		{
			"origin",
			"refs/heads/*:refs/remotes/origin/*",
			true,
			map[string]string{
				"refs/heads/master":          "refs/remotes/origin/master",
				"refs/heads/feature":         "refs/remotes/origin/feature",
				"refs/remotes/origin/master": "refs/invalid/",
			},
		}, {
			"borigin",
			"refs/heads/master:refs/remotes/borigin/mymaster",
			true,
			map[string]string{
				"refs/heads/master":  "refs/remotes/borigin/mymaster",
				"refs/heads/feature": "refs/invalid/",
			},
		}, {
			"borigin",
			"refs/heads/*/master:refs/remotes/borigin/*/mymaster",
			true,
			map[string]string{
				"refs/heads/master":    "refs/invalid/",
				"refs/heads/bh/master": "refs/remotes/borigin/bh/mymaster",
				"refs/heads/as/master": "refs/remotes/borigin/as/mymaster",
			},
		}, {
			"origin",
			"refs/heads/master:refs/remotes/not_borigin/mymaster",
			false,
			nil,
		}, {
			"origin",
			"refs/heads/*:refs/remotes/origin/branchname",
			false,
			nil,
		}, {
			"origin",
			"refs/heads/branchname:refs/remotes/origin/*",
			false,
			nil,
		},
		{
			"origin",
			"refs/heads/*/*:refs/remotes/origin/*/*",
			false,
			nil,
		},
	}

	for _, test := range tests {
		refSpec, err := ParseRefSpecForRemote(test.remote, test.refSpecStr)

		if (err == nil) != test.isValid {
			t.Error(test.refSpecStr, "is valid:", err == nil)
		} else if err == nil {
			for in, out := range test.inToExpOut {
				inRef, _ := Parse(in)
				outRef, _ := Parse(out)

				actual := refSpec.Map(inRef)

				if !actual.Equals(outRef) {
					t.Error(test.refSpecStr, "mapped", in, "to", actual.String(), "expected", outRef.String())
				}
			}
		}
	}
}
