package actions

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	testHomeDir = "/user/bheni"
	workingDir  = "/user/bheni/datasets/addresses"
	credsDir    = "creds"

	configFile       = "config.json"
	GlobalConfigFile = "config_global.json"
)

func testHomeDirFunc() (string, error) {
	return testHomeDir, nil
}

func createTestEnv() (*env.DoltEnv, *filesys.InMemFS) {
	initialDirs := []string{testHomeDir, workingDir}
	initialFiles := map[string][]byte{}

	fs := filesys.NewInMemFS(initialDirs, initialFiles, workingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")

	return dEnv, fs
}

func TestVisitResolvedTag(t *testing.T) {
	dEnv, _ := createTestEnv()
	ctx := context.Background()

	// Initialize repo
	err := dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main")
	require.NoError(t, err)

	// Create a tag
	tagName := "test-tag"
	tagMsg := "test tag message"
	err = CreateTag(ctx, dEnv, tagName, "main", TagProps{TaggerName: "test user", TaggerEmail: "test@test.com", Description: tagMsg})
	require.NoError(t, err)

	// Visit the tag and verify its properties
	var foundTag *doltdb.Tag
	err = VisitResolvedTag(ctx, dEnv.DoltDB, tagName, func(tag *doltdb.Tag) error {
		foundTag = tag
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, foundTag)
	require.Equal(t, tagName, foundTag.Name)
	require.Equal(t, tagMsg, foundTag.Meta.Description)

	// Test visiting non-existent tag
	err = VisitResolvedTag(ctx, dEnv.DoltDB, "non-existent-tag", func(tag *doltdb.Tag) error {
		return nil
	})
	require.Equal(t, doltdb.ErrTagNotFound, err)
}

func TestIterResolvedTagsPaginated(t *testing.T) {
	dEnv, _ := createTestEnv()
	ctx := context.Background()

	// Initialize repo
	err := dEnv.InitRepo(ctx, types.Format_Default, "test user", "test@test.com", "main")
	require.NoError(t, err)

	expectedTagNames := make([]string, DefaultPageSize*2)
	// Create multiple tags with different timestamps
	tagNames := make([]string, DefaultPageSize*2)
	for i := range tagNames {
		tagName := fmt.Sprintf("tag-%d", i)
		err = CreateTag(ctx, dEnv, tagName, "main", TagProps{
			TaggerName:  "test user",
			TaggerEmail: "test@test.com",
			Description: fmt.Sprintf("test tag %s", tagName),
		})
		time.Sleep(2 * time.Millisecond)
		require.NoError(t, err)
		tagNames[i] = tagName
		expectedTagNames[len(expectedTagNames)-i-1] = tagName
	}

	// Test first page
	var foundTags []string
	pageToken, err := IterResolvedTagsPaginated(ctx, dEnv.DoltDB, "", func(tag *doltdb.Tag) (bool, error) {
		foundTags = append(foundTags, tag.Name)
		return false, nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, pageToken)                    // Should have next page
	require.Equal(t, DefaultPageSize, len(foundTags)) // Default page size tags returned

	// Test second page
	var secondPageTags []string
	nextPageToken, err := IterResolvedTagsPaginated(ctx, dEnv.DoltDB, pageToken, func(tag *doltdb.Tag) (bool, error) {
		secondPageTags = append(secondPageTags, tag.Name)
		return false, nil
	})
	require.NoError(t, err)
	require.Empty(t, nextPageToken)                        // Should be no more pages
	require.Equal(t, DefaultPageSize, len(secondPageTags)) // Remaining tags

	// Verify all tags were found
	allFoundTags := append(foundTags, secondPageTags...)
	require.Equal(t, len(tagNames), len(allFoundTags))
	require.Equal(t, expectedTagNames, allFoundTags)

	// Test early termination
	var earlyTermTags []string
	_, err = IterResolvedTagsPaginated(ctx, dEnv.DoltDB, "", func(tag *doltdb.Tag) (bool, error) {
		earlyTermTags = append(earlyTermTags, tag.Name)
		return true, nil // Stop after first tag
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(earlyTermTags))
}
