// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbfactory

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAWSPathValidation(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		expectedPath string
		expectErr    bool
	}{
		{
			"empty path",
			"",
			"",
			true,
		},
		{
			"basic",
			"database",
			"database",
			false,
		},
		{
			"slash prefix",
			"/database",
			"database",
			false,
		},
		{
			"slash suffix",
			"database/",
			"database",
			false,
		},
		{
			"slash prefix and suffix",
			"/database/",
			"database",
			false,
		},
		{
			"slash in the middle",
			"/data/base/",
			"data/base",
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualPath, actualErr := validatePath(test.path)

			assert.Equal(t, actualPath, test.expectedPath)

			if test.expectErr {
				assert.Error(t, actualErr, "Did not expect an error")
			} else {
				assert.NoError(t, actualErr, "Expected an error")
			}
		})
	}
}

// This test asserts some of the behaviors of AWS client configuration
// through the DB parameters. These parameters are typically passed on
// as config on dolt remotes or backups.
//
// The behavior specified here is somewhat inherited from Nom's
// store/spec. It is not necessarily ideal, but it how Dolt behaves
// today.
//
// These tests are not Parallel safe, since they modify the
// environment of the running test process.
func TestAWSConfigFromParams(t *testing.T) {
	// XXX: These must match the contents of the files in testdata/
	const loadFromFileProfileRegion = "il-central-1"
	const loadFromFileProfileAccessKeyID = "2A16B802-306B-43CB-B3A7-8A2DAB712F44"
	const loadFromFileProfileSecretAccessKey = "28893871-7BEB-4AE3-B643-1DB21FCB9EC8"
	const defaultProfileAccessKeyID = "BEFB28DF-A5AA-423C-A09B-35583580D740"
	const defaultProfileSecretAccessKey = "8E44AE62-AC85-4C9D-BE48-802AA081EBF3"
	const defaultProfileRegion = "eu-central-1"
	const onlyCredsProfileAccessKeyID = "4A5A28FB-35CF-44FD-8344-A581EDC970BA"
	const onlyCredsProfileSecretAccessKey = "86D705DE-A73C-4B13-9690-CAE285C40793"

	setEnv := func(t *testing.T, env map[string]string) {
		orig := make(map[string]string, len(env))
		for k := range env {
			orig[k] = os.Getenv(k)
		}
		t.Cleanup(func() {
			for k, v := range orig {
				os.Setenv(k, v)
			}
		})
		for k, v := range env {
			os.Setenv(k, v)
		}
	}
	getSession := func(t *testing.T, params map[string]interface{}) *session.Session {
		opts, err := awsConfigFromParams(params)
		require.NoError(t, err)
		sess, err := session.NewSessionWithOptions(opts)
		require.NoError(t, err)
		return sess
	}

	// Do not pick up config from any files in the running user's
	// home directory (potentially a developer).
	setEnv(t, map[string]string{
		"HOME": "/does_not_exist",
	})

	cwd, err := os.Getwd()
	require.NoError(t, err)

	type roleTypeTest struct {
		name     string
		passType bool
	}
	getRttParams := func(rtt roleTypeTest) map[string]interface{} {
		ret := make(map[string]interface{})
		if rtt.passType {
			ret[AWSCredsTypeParam] = "role"
		}
		return ret
	}
	for _, rtt := range []roleTypeTest{{
		name:     "NoType",
		passType: false,
	}, {
		name:     "RoleType",
		passType: true,
	}} {
		t.Run(rtt.name, func(t *testing.T) {
			t.Run("CredsInEnv", func(t *testing.T) {
				expectedAccessKeyID := uuid.New().String()
				expectedSecretAccessKey := uuid.New().String()
				expectedRegion := "us-west-2"
				setEnv(t, map[string]string{
					"AWS_ACCESS_KEY_ID":     expectedAccessKeyID,
					"AWS_SECRET_ACCESS_KEY": expectedSecretAccessKey,
					"AWS_REGION":            expectedRegion,
				})
				sess := getSession(t, getRttParams(rtt))
				creds, err := sess.Config.Credentials.Get()
				if assert.NoError(t, err) {
					assert.Equal(t, expectedAccessKeyID, creds.AccessKeyID)
					assert.Equal(t, expectedSecretAccessKey, creds.SecretAccessKey)
				}
				if assert.NotNil(t, sess.Config.Region) {
					assert.Equal(t, expectedRegion, *sess.Config.Region)
				}
			})
			t.Run("CredsInLegacyEnv", func(t *testing.T) {
				expectedAccessKeyID := uuid.New().String()
				expectedSecretAccessKey := uuid.New().String()
				expectedRegion := "us-west-2"
				setEnv(t, map[string]string{
					"AWS_ACCESS_KEY":     expectedAccessKeyID,
					"AWS_SECRET_KEY":     expectedSecretAccessKey,
					"AWS_DEFAULT_REGION": expectedRegion,
				})
				sess := getSession(t, getRttParams(rtt))
				creds, err := sess.Config.Credentials.Get()
				if assert.NoError(t, err) {
					assert.Equal(t, expectedAccessKeyID, creds.AccessKeyID)
					assert.Equal(t, expectedSecretAccessKey, creds.SecretAccessKey)
				}
				if assert.NotNil(t, sess.Config.Region) {
					assert.Equal(t, expectedRegion, *sess.Config.Region)
				}
			})
			t.Run("FilesInEnv", func(t *testing.T) {
				t.Run("ProfileInEnv", func(t *testing.T) {
					loadedProfile := "load_from_file"
					configFile := filepath.Join(cwd, "testdata", "basic_config_file")
					credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
					setEnv(t, map[string]string{
						"AWS_PROFILE":                 loadedProfile,
						"AWS_CONFIG_FILE":             configFile,
						"AWS_SHARED_CREDENTIALS_FILE": credsFile,
					})
					sess := getSession(t, getRttParams(rtt))
					creds, err := sess.Config.Credentials.Get()
					if assert.NoError(t, err) {
						assert.Equal(t, loadFromFileProfileAccessKeyID, creds.AccessKeyID)
						assert.Equal(t, loadFromFileProfileSecretAccessKey, creds.SecretAccessKey)
					}
					if assert.NotNil(t, sess.Config.Region) {
						assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
					}
				})
				t.Run("ProfileInLegacyEnv", func(t *testing.T) {
					loadedProfile := "load_from_file"
					configFile := filepath.Join(cwd, "testdata", "basic_config_file")
					credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
					setEnv(t, map[string]string{
						"AWS_DEFAULT_PROFILE":         loadedProfile,
						"AWS_CONFIG_FILE":             configFile,
						"AWS_SHARED_CREDENTIALS_FILE": credsFile,
					})
					sess := getSession(t, getRttParams(rtt))
					creds, err := sess.Config.Credentials.Get()
					if assert.NoError(t, err) {
						assert.Equal(t, loadFromFileProfileAccessKeyID, creds.AccessKeyID)
						assert.Equal(t, loadFromFileProfileSecretAccessKey, creds.SecretAccessKey)
					}
					if assert.NotNil(t, sess.Config.Region) {
						assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
					}
				})
				t.Run("ProfileInParam", func(t *testing.T) {
					loadedProfile := "load_from_file"
					configFile := filepath.Join(cwd, "testdata", "basic_config_file")
					credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
					setEnv(t, map[string]string{
						"AWS_CONFIG_FILE":             configFile,
						"AWS_SHARED_CREDENTIALS_FILE": credsFile,
					})
					params := getRttParams(rtt)
					params[AWSCredsProfile] = loadedProfile
					sess := getSession(t, params)
					creds, err := sess.Config.Credentials.Get()
					if assert.NoError(t, err) {
						assert.Equal(t, loadFromFileProfileAccessKeyID, creds.AccessKeyID)
						assert.Equal(t, loadFromFileProfileSecretAccessKey, creds.SecretAccessKey)
					}
					if assert.NotNil(t, sess.Config.Region) {
						assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
					}
				})
				t.Run("FileParamOverridesCredsTypeRole", func(t *testing.T) {
					// If an aws-creds-file parameter is passed,
					// the credentials are forcefully loaded from
					// there, ignoring the default credentials
					// chain. AWS_CONFIG_FILE still works for region.
					envAccessKeyID := uuid.New().String()
					envSecretAccessKeyID := uuid.New().String()
					loadedProfile := "load_from_file"
					configFile := filepath.Join(cwd, "testdata", "basic_config_file")
					altCredsFile := filepath.Join(cwd, "testdata", "alt_creds_file")
					setEnv(t, map[string]string{
						"AWS_ACCESS_KEY":              envAccessKeyID,
						"AWS_SECRET_KEY":              envSecretAccessKeyID,
						"AWS_CONFIG_FILE":             configFile,
						"AWS_SHARED_CREDENTIALS_FILE": altCredsFile,
					})
					credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
					params := getRttParams(rtt)
					params[AWSCredsProfile] = loadedProfile
					params[AWSCredsFileParam] = credsFile
					sess := getSession(t, params)
					creds, err := sess.Config.Credentials.Get()
					if assert.NoError(t, err) {
						assert.Equal(t, loadFromFileProfileAccessKeyID, creds.AccessKeyID)
						assert.Equal(t, loadFromFileProfileSecretAccessKey, creds.SecretAccessKey)
					}
					if assert.NotNil(t, sess.Config.Region) {
						assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
					}
				})
				// XXX: Currently there are no tests
				// here of web identity token
				// authentication, assume role
				// profiles, instance role credentials
				// from IMDSv2 or SSO credentials, but
				// those are all supported through
				// aws-creds-type: role / the default
				// aws-creds-type (as long as
				// aws-creds-file is not passed...).
				//
				// Potentially a major shortcoming of
				// the current implementation is that
				// there is no way to configure a
				// per-remote profile config file, and
				// attempting to configure a
				// per-remote credentials file
				// overrides the creds-type to be
				// file, which is restricted to just
				// static credentials in the form of
				// aws_access_key_id and
				// aws_access_secret_key being set
				// directly in the INI stanza.
			})
		})
	}
	t.Run("RegionParamOverridesEnv", func(t *testing.T) {
		expectedAccessKeyID := uuid.New().String()
		expectedSecretAccessKey := uuid.New().String()
		envRegion := "us-west-2"
		expectedRegion := "us-east-1"
		setEnv(t, map[string]string{
			"AWS_ACCESS_KEY_ID":     expectedAccessKeyID,
			"AWS_SECRET_ACCESS_KEY": expectedSecretAccessKey,
			"AWS_REGION":            envRegion,
		})
		sess := getSession(t, map[string]interface{}{
			AWSRegionParam: expectedRegion,
		})
		creds, err := sess.Config.Credentials.Get()
		if assert.NoError(t, err) {
			assert.Equal(t, expectedAccessKeyID, creds.AccessKeyID)
			assert.Equal(t, expectedSecretAccessKey, creds.SecretAccessKey)
		}
		if assert.NotNil(t, sess.Config.Region) {
			assert.Equal(t, expectedRegion, *sess.Config.Region)
		}
	})
	t.Run("CredsTypeEnv", func(t *testing.T) {
		t.Run("PopulatedCreds", func(t *testing.T) {
			expectedAccessKeyID := uuid.New().String()
			expectedSecretAccessKey := uuid.New().String()
			expectedRegion := "us-west-2"
			setEnv(t, map[string]string{
				"AWS_ACCESS_KEY_ID":     expectedAccessKeyID,
				"AWS_SECRET_ACCESS_KEY": expectedSecretAccessKey,
				"AWS_REGION":            expectedRegion,
			})
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "env",
			})
			creds, err := sess.Config.Credentials.Get()
			if assert.NoError(t, err) {
				assert.Equal(t, expectedAccessKeyID, creds.AccessKeyID)
				assert.Equal(t, expectedSecretAccessKey, creds.SecretAccessKey)
			}
			if assert.NotNil(t, sess.Config.Region) {
				assert.Equal(t, expectedRegion, *sess.Config.Region)
			}
		})
		t.Run("MissingAccessKeyID", func(t *testing.T) {
			expectedSecretAccessKey := uuid.New().String()
			expectedRegion := "us-west-2"
			setEnv(t, map[string]string{
				"AWS_SECRET_ACCESS_KEY": expectedSecretAccessKey,
				"AWS_REGION":            expectedRegion,
			})
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "env",
			})
			_, err := sess.Config.Credentials.Get()
			require.Error(t, err)
		})
		t.Run("MissingSecretAccessKey", func(t *testing.T) {
			expectedAccessKeyID := uuid.New().String()
			expectedRegion := "us-west-2"
			setEnv(t, map[string]string{
				"AWS_ACCESS_KEY_ID": expectedAccessKeyID,
				"AWS_REGION":        expectedRegion,
			})
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "env",
			})
			_, err := sess.Config.Credentials.Get()
			require.Error(t, err)
		})
	})
	t.Run("CredsTypeFile", func(t *testing.T) {
		t.Run("FileParamDoesNotExist", func(t *testing.T) {
			_, err := awsConfigFromParams(map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsProfile:   "some_profile",
			})
			require.Error(t, err)
		})
		t.Run("FileDoesNotExist", func(t *testing.T) {
			loadedProfile := "default"
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			credsFile := uuid.New().String()
			setEnv(t, map[string]string{
				"AWS_CONFIG_FILE": configFile,
			})
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsProfile:   loadedProfile,
				AWSCredsFileParam: credsFile,
			})
			_, err = sess.Config.Credentials.Get()
			require.Error(t, err)
		})
		t.Run("ProfileFromParamDoesNotExist", func(t *testing.T) {
			loadedProfile := "does_not_exist"
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
			setEnv(t, map[string]string{
				"AWS_CONFIG_FILE": configFile,
			})
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsProfile:   loadedProfile,
				AWSCredsFileParam: credsFile,
			})
			_, err = sess.Config.Credentials.Get()
			require.Error(t, err)
		})
		t.Run("ProfileFromEnvDoesNotExist", func(t *testing.T) {
			loadedProfile := "does_not_exist"
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
			setEnv(t, map[string]string{
				"AWS_CONFIG_FILE": configFile,
				"AWS_PROFILE":     loadedProfile,
			})
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsFileParam: credsFile,
			})
			_, err = sess.Config.Credentials.Get()
			require.Error(t, err)
		})
		type profileOnlyHasCredsTest struct {
			name    string
			fileEnv string
		}
		allProfileOnlyHasCredsTests := []profileOnlyHasCredsTest{{
			name:    "SpecifyExistingConfigFile",
			fileEnv: filepath.Join(cwd, "testdata", "basic_config_file"),
		}, {
			name:    "SpecifyNonExistantConfigFile",
			fileEnv: "/dev/null/does_not_exist",
		}, {
			name: "DoNotSpecifyConfigFile",
		}}
		t.Run("ProfileFromEnvOnlyExistsInCreds", func(t *testing.T) {
			for _, tt := range allProfileOnlyHasCredsTests {
				t.Run(tt.name, func(t *testing.T) {
					loadedProfile := "only_creds_profile"
					credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
					setEnv(t, map[string]string{
						"AWS_PROFILE": loadedProfile,
					})
					if tt.fileEnv != "" {
						setEnv(t, map[string]string{
							"AWS_CONFIG_FILE": tt.fileEnv,
						})
					}
					sess := getSession(t, map[string]interface{}{
						AWSCredsTypeParam: "file",
						AWSCredsFileParam: credsFile,
					})
					creds, err := sess.Config.Credentials.Get()
					if assert.NoError(t, err) {
						assert.Equal(t, onlyCredsProfileAccessKeyID, creds.AccessKeyID)
						assert.Equal(t, onlyCredsProfileSecretAccessKey, creds.SecretAccessKey)
					}
				})
			}
		})
		t.Run("ProfileFromParamOnlyExistsInCreds", func(t *testing.T) {
			for _, tt := range allProfileOnlyHasCredsTests {
				t.Run(tt.name, func(t *testing.T) {
					loadedProfile := "only_creds_profile"
					credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
					if tt.fileEnv != "" {
						setEnv(t, map[string]string{
							"AWS_CONFIG_FILE": tt.fileEnv,
						})
					}
					sess := getSession(t, map[string]interface{}{
						AWSCredsTypeParam: "file",
						AWSCredsFileParam: credsFile,
						AWSCredsProfile:   loadedProfile,
					})
					creds, err := sess.Config.Credentials.Get()
					if assert.NoError(t, err) {
						assert.Equal(t, onlyCredsProfileAccessKeyID, creds.AccessKeyID)
						assert.Equal(t, onlyCredsProfileSecretAccessKey, creds.SecretAccessKey)
					}
				})
			}
		})
		t.Run("IgnoresEnv", func(t *testing.T) {
			envAccessKeyID := uuid.New().String()
			envSecretAccessKeyID := uuid.New().String()
			loadedProfile := "load_from_file"
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			altCredsFile := filepath.Join(cwd, "testdata", "alt_creds_file")
			setEnv(t, map[string]string{
				"AWS_ACCESS_KEY":              envAccessKeyID,
				"AWS_SECRET_KEY":              envSecretAccessKeyID,
				"AWS_CONFIG_FILE":             configFile,
				"AWS_SHARED_CREDENTIALS_FILE": altCredsFile,
			})
			credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsProfile:   loadedProfile,
				AWSCredsFileParam: credsFile,
			})
			creds, err := sess.Config.Credentials.Get()
			if assert.NoError(t, err) {
				assert.Equal(t, loadFromFileProfileAccessKeyID, creds.AccessKeyID)
				assert.Equal(t, loadFromFileProfileSecretAccessKey, creds.SecretAccessKey)
			}
			if assert.NotNil(t, sess.Config.Region) {
				assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
			}
		})
		t.Run("NoProfileUsesDefault", func(t *testing.T) {
			// If no aws-profile parameter is supplied,
			// and no AWS_PROFILE is set, then "default"
			// is used for both config and credentials.
			envAccessKeyID := uuid.New().String()
			envSecretAccessKeyID := uuid.New().String()
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			altCredsFile := filepath.Join(cwd, "testdata", "alt_creds_file")
			setEnv(t, map[string]string{
				"AWS_ACCESS_KEY":              envAccessKeyID,
				"AWS_SECRET_KEY":              envSecretAccessKeyID,
				"AWS_CONFIG_FILE":             configFile,
				"AWS_SHARED_CREDENTIALS_FILE": altCredsFile,
			})
			credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsFileParam: credsFile,
			})
			creds, err := sess.Config.Credentials.Get()
			if assert.NoError(t, err) {
				assert.Equal(t, defaultProfileAccessKeyID, creds.AccessKeyID)
				assert.Equal(t, defaultProfileSecretAccessKey, creds.SecretAccessKey)
			}
			if assert.NotNil(t, sess.Config.Region) {
				assert.Equal(t, defaultProfileRegion, *sess.Config.Region)
			}
		})
		t.Run("ProfileInEnv", func(t *testing.T) {
			// If no aws-profile parameter is supplied,
			// then AWS_PROFILE is used for both config
			// and credentials.
			envAccessKeyID := uuid.New().String()
			envSecretAccessKeyID := uuid.New().String()
			loadedProfile := "load_from_file"
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			altCredsFile := filepath.Join(cwd, "testdata", "alt_creds_file")
			setEnv(t, map[string]string{
				"AWS_PROFILE":                 loadedProfile,
				"AWS_ACCESS_KEY":              envAccessKeyID,
				"AWS_SECRET_KEY":              envSecretAccessKeyID,
				"AWS_CONFIG_FILE":             configFile,
				"AWS_SHARED_CREDENTIALS_FILE": altCredsFile,
			})
			credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsFileParam: credsFile,
			})
			creds, err := sess.Config.Credentials.Get()
			if assert.NoError(t, err) {
				assert.Equal(t, loadFromFileProfileAccessKeyID, creds.AccessKeyID)
				assert.Equal(t, loadFromFileProfileSecretAccessKey, creds.SecretAccessKey)
			}
			if assert.NotNil(t, sess.Config.Region) {
				assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
			}
		})
		t.Run("SplitBrainProfileInLegacyEnv", func(t *testing.T) {
			// If no aws-profile parameter is supplied,
			// and AWS_PROFILE is not supplied, but
			// AWS_DEFAULT_PROFILE is, then config is
			// loaded from AWS_DEFAULT_PROFILE but
			// credentials are loaded from the profile
			// "default"
			//
			// This is a weird, probably unintentional
			// edge case which we probably don't want to
			// support.
			envAccessKeyID := uuid.New().String()
			envSecretAccessKeyID := uuid.New().String()
			loadedProfile := "load_from_file"
			configFile := filepath.Join(cwd, "testdata", "basic_config_file")
			altCredsFile := filepath.Join(cwd, "testdata", "alt_creds_file")
			setEnv(t, map[string]string{
				"AWS_DEFAULT_PROFILE":         loadedProfile,
				"AWS_ACCESS_KEY":              envAccessKeyID,
				"AWS_SECRET_KEY":              envSecretAccessKeyID,
				"AWS_CONFIG_FILE":             configFile,
				"AWS_SHARED_CREDENTIALS_FILE": altCredsFile,
			})
			credsFile := filepath.Join(cwd, "testdata", "basic_creds_file")
			sess := getSession(t, map[string]interface{}{
				AWSCredsTypeParam: "file",
				AWSCredsFileParam: credsFile,
			})
			creds, err := sess.Config.Credentials.Get()
			if assert.NoError(t, err) {
				assert.Equal(t, defaultProfileAccessKeyID, creds.AccessKeyID)
				assert.Equal(t, defaultProfileSecretAccessKey, creds.SecretAccessKey)
			}
			if assert.NotNil(t, sess.Config.Region) {
				assert.Equal(t, loadFromFileProfileRegion, *sess.Config.Region)
			}
		})
	})
	t.Run("CredentialsFileRefresh", func(t *testing.T) {
		// One of the particularly salient features of
		// "aws-creds-type": "file" is that it periodically
		// refreshes the credentials from the file. This
		// feature is used to deliver updating attenuated
		// credentials to a running dotl sql-server in certain
		// hosted environments.
		dir := t.TempDir()
		origDuration := AWSFileCredsRefreshDuration
		t.Cleanup(func() {
			AWSFileCredsRefreshDuration = origDuration
		})
		AWSFileCredsRefreshDuration = time.Millisecond
		credsFilePath := filepath.Join(dir, "creds_file")
		credsFileContents := []byte(`
[some_profile]
aws_access_key_id = original_access_key_id
aws_secret_access_key = original_secret_access_key
`)
		newCredsFileContents := []byte(`
[some_profile]
aws_access_key_id = new_access_key_id
aws_secret_access_key = new_secret_access_key
`)
		require.NoError(t, os.WriteFile(credsFilePath, credsFileContents, 0660))
		sess := getSession(t, map[string]interface{}{
			AWSCredsTypeParam: "file",
			AWSCredsFileParam: credsFilePath,
			AWSRegionParam:    "us-west-2",
			AWSCredsProfile:   "some_profile",
		})
		if assert.NotNil(t, sess.Config.Region) {
			assert.Equal(t, "us-west-2", *sess.Config.Region)
		}
		creds, err := sess.Config.Credentials.Get()
		if assert.NoError(t, err) {
			assert.Equal(t, "original_access_key_id", creds.AccessKeyID)
			assert.Equal(t, "original_secret_access_key", creds.SecretAccessKey)
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "new_creds_file"), newCredsFileContents, 0660))
		require.NoError(t, os.Rename(filepath.Join(dir, "new_creds_file"), credsFilePath))
		time.Sleep(10 * time.Millisecond)
		creds, err = sess.Config.Credentials.Get()
		if assert.NoError(t, err) {
			assert.Equal(t, "new_access_key_id", creds.AccessKeyID)
			assert.Equal(t, "new_secret_access_key", creds.SecretAccessKey)
		}
	})
}
