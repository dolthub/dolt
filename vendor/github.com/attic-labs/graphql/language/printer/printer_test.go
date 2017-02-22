package printer_test

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/parser"
	"github.com/attic-labs/graphql/language/printer"
	"github.com/attic-labs/graphql/testutil"
)

func parse(t *testing.T, query string) *ast.Document {
	astDoc, err := parser.Parse(parser.ParseParams{
		Source: query,
		Options: parser.ParseOptions{
			NoLocation: true,
		},
	})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return astDoc
}

func TestPrinter_DoesNotAlterAST(t *testing.T) {
	b, err := ioutil.ReadFile("../../kitchen-sink.graphql")
	if err != nil {
		t.Fatalf("unable to load kitchen-sink.graphql")
	}

	query := string(b)
	astDoc := parse(t, query)

	astDocBefore := testutil.ASTToJSON(t, astDoc)

	_ = printer.Print(astDoc)

	astDocAfter := testutil.ASTToJSON(t, astDoc)

	_ = testutil.ASTToJSON(t, astDoc)

	if !reflect.DeepEqual(astDocAfter, astDocBefore) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(astDocAfter, astDocBefore))
	}
}

func TestPrinter_PrintsMinimalAST(t *testing.T) {
	astDoc := ast.NewField(&ast.Field{
		Name: ast.NewName(&ast.Name{
			Value: "foo",
		}),
	})
	results := printer.Print(astDoc)
	expected := "foo"
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(expected, results))
	}
}

// TestPrinter_ProducesHelpfulErrorMessages
// Skipped, can't figure out how to pass in an invalid astDoc, which is already strongly-typed

func TestPrinter_CorrectlyPrintsNonQueryOperationsWithoutName(t *testing.T) {

	// Test #1
	queryAstShorthanded := `query { id, name }`
	expected := `{
  id
  name
}
`
	astDoc := parse(t, queryAstShorthanded)
	results := printer.Print(astDoc)

	if !reflect.DeepEqual(expected, results) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(results, expected))
	}

	// Test #2
	mutationAst := `mutation { id, name }`
	expected = `mutation {
  id
  name
}
`
	astDoc = parse(t, mutationAst)
	results = printer.Print(astDoc)

	if !reflect.DeepEqual(expected, results) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(results, expected))
	}

	// Test #3
	queryAstWithArtifacts := `query ($foo: TestType) @testDirective { id, name }`
	expected = `query ($foo: TestType) @testDirective {
  id
  name
}
`
	astDoc = parse(t, queryAstWithArtifacts)
	results = printer.Print(astDoc)

	if !reflect.DeepEqual(expected, results) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(results, expected))
	}

	// Test #4
	mutationAstWithArtifacts := `mutation ($foo: TestType) @testDirective { id, name }`
	expected = `mutation ($foo: TestType) @testDirective {
  id
  name
}
`
	astDoc = parse(t, mutationAstWithArtifacts)
	results = printer.Print(astDoc)

	if !reflect.DeepEqual(expected, results) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(results, expected))
	}
}

func TestPrinter_PrintsKitchenSink(t *testing.T) {
	b, err := ioutil.ReadFile("../../kitchen-sink.graphql")
	if err != nil {
		t.Fatalf("unable to load kitchen-sink.graphql")
	}

	query := string(b)
	astDoc := parse(t, query)
	expected := `query namedQuery($foo: ComplexFooType, $bar: Bar = DefaultBarValue) {
  customUser: user(id: [987, 654]) {
    id
    ... on User @defer {
      field2 {
        id
        alias: field1(first: 10, after: $foo) @include(if: $foo) {
          id
          ...frag
        }
      }
    }
    ... @skip(unless: $foo) {
      id
    }
    ... {
      id
    }
  }
}

mutation favPost {
  fav(post: 123) @defer {
    post {
      id
    }
  }
}

subscription PostFavSubscription($input: StoryLikeSubscribeInput) {
  postFavSubscribe(input: $input) {
    post {
      favers {
        count
      }
      favSentence {
        text
      }
    }
  }
}

fragment frag on Follower {
  foo(size: $size, bar: $b, obj: {key: "value"})
}

{
  unnamed(truthyVal: true, falseyVal: false)
  query
}
`
	results := printer.Print(astDoc)

	if !reflect.DeepEqual(expected, results) {
		t.Fatalf("Unexpected result, Diff: %v", testutil.Diff(results, expected))
	}
}
