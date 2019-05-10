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
				"refs/remotes/origin/master": "refs/nil/",
			},
		}, {
			"borigin",
			"refs/heads/master:refs/remotes/borigin/mymaster",
			true,
			map[string]string{
				"refs/heads/master":  "refs/remotes/borigin/mymaster",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			"",
			"refs/heads/*/master:refs/remotes/borigin/*/mymaster",
			true,
			map[string]string{
				"refs/heads/master":    "refs/nil/",
				"refs/heads/bh/master": "refs/remotes/borigin/bh/mymaster",
				"refs/heads/as/master": "refs/remotes/borigin/as/mymaster",
			},
		}, {
			"",
			"master",
			true,
			map[string]string{
				"refs/heads/master":  "refs/heads/master",
				"refs/heads/feature": "refs/nil/",
			},
		}, {
			"",
			"master:master",
			true,
			map[string]string{
				"refs/heads/master":  "refs/heads/master",
				"refs/heads/feature": "refs/nil/",
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
		}, {
			"origin",
			"refs/heads/*/*:refs/remotes/origin/*/*",
			false,
			nil,
		},
	}

	for _, test := range tests {
		var refSpec RefSpec
		var err error

		if test.remote == "" {
			refSpec, err = ParseRefSpec(test.refSpecStr)
		} else {
			refSpec, err = ParseRefSpecForRemote(test.remote, test.refSpecStr)
		}

		if (err == nil) != test.isValid {
			t.Error(test.refSpecStr, "is valid:", err == nil)
		} else if err == nil {
			for in, out := range test.inToExpOut {
				inRef, _ := Parse(in)
				outRef, _ := Parse(out)

				actual := refSpec.DestRef(inRef)

				if !Equals(actual, outRef) {
					t.Error(test.refSpecStr, "mapped", in, "to", actual.String(), "expected", outRef.String())
				}
			}
		}
	}
}
