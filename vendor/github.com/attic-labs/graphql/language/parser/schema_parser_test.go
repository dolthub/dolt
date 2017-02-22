package parser

import (
	"reflect"
	"testing"

	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/ast"
	"github.com/attic-labs/graphql/language/location"
	"github.com/attic-labs/graphql/language/source"
)

func parse(t *testing.T, query string) *ast.Document {
	astDoc, err := Parse(ParseParams{
		Source: query,
		Options: ParseOptions{
			NoLocation: false,
			NoSource:   true,
		},
	})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return astDoc
}

func testLoc(start int, end int) *ast.Location {
	return &ast.Location{
		Start: start, End: end,
	}
}
func TestSchemaParser_SimpleType(t *testing.T) {

	body := `
type Hello {
  world: String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 31),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(1, 31),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Interfaces: []*ast.Named{},
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(16, 29),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(16, 21),
						}),
						Arguments: []*ast.InputValueDefinition{},
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(23, 29),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(23, 29),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleExtension(t *testing.T) {

	body := `
extend type Hello {
  world: String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 38),
		Definitions: []ast.Node{
			ast.NewTypeExtensionDefinition(&ast.TypeExtensionDefinition{
				Loc: testLoc(1, 38),
				Definition: ast.NewObjectDefinition(&ast.ObjectDefinition{
					Loc: testLoc(8, 38),
					Name: ast.NewName(&ast.Name{
						Value: "Hello",
						Loc:   testLoc(13, 18),
					}),
					Interfaces: []*ast.Named{},
					Fields: []*ast.FieldDefinition{
						ast.NewFieldDefinition(&ast.FieldDefinition{
							Loc: testLoc(23, 36),
							Name: ast.NewName(&ast.Name{
								Value: "world",
								Loc:   testLoc(23, 28),
							}),
							Arguments: []*ast.InputValueDefinition{},
							Type: ast.NewNamed(&ast.Named{
								Loc: testLoc(30, 36),
								Name: ast.NewName(&ast.Name{
									Value: "String",
									Loc:   testLoc(30, 36),
								}),
							}),
						}),
					},
				}),
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleNonNullType(t *testing.T) {

	body := `
type Hello {
  world: String!
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 32),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(1, 32),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Interfaces: []*ast.Named{},
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(16, 30),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(16, 21),
						}),
						Arguments: []*ast.InputValueDefinition{},
						Type: ast.NewNonNull(&ast.NonNull{
							Kind: "NonNullType",
							Loc:  testLoc(23, 30),
							Type: ast.NewNamed(&ast.Named{
								Loc: testLoc(23, 29),
								Name: ast.NewName(&ast.Name{
									Value: "String",
									Loc:   testLoc(23, 29),
								}),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleTypeInheritingInterface(t *testing.T) {
	body := `type Hello implements World { }`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 31),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(0, 31),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(5, 10),
				}),
				Interfaces: []*ast.Named{
					ast.NewNamed(&ast.Named{
						Name: ast.NewName(&ast.Name{
							Value: "World",
							Loc:   testLoc(22, 27),
						}),
						Loc: testLoc(22, 27),
					}),
				},
				Fields: []*ast.FieldDefinition{},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleTypeInheritingMultipleInterfaces(t *testing.T) {
	body := `type Hello implements Wo, rld { }`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 33),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(0, 33),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(5, 10),
				}),
				Interfaces: []*ast.Named{
					ast.NewNamed(&ast.Named{
						Name: ast.NewName(&ast.Name{
							Value: "Wo",
							Loc:   testLoc(22, 24),
						}),
						Loc: testLoc(22, 24),
					}),
					ast.NewNamed(&ast.Named{
						Name: ast.NewName(&ast.Name{
							Value: "rld",
							Loc:   testLoc(26, 29),
						}),
						Loc: testLoc(26, 29),
					}),
				},
				Fields: []*ast.FieldDefinition{},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SingleValueEnum(t *testing.T) {
	body := `enum Hello { WORLD }`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 20),
		Definitions: []ast.Node{
			ast.NewEnumDefinition(&ast.EnumDefinition{
				Loc: testLoc(0, 20),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(5, 10),
				}),
				Values: []*ast.EnumValueDefinition{
					ast.NewEnumValueDefinition(&ast.EnumValueDefinition{
						Name: ast.NewName(&ast.Name{
							Value: "WORLD",
							Loc:   testLoc(13, 18),
						}),
						Loc: testLoc(13, 18),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_DoubleValueEnum(t *testing.T) {
	body := `enum Hello { WO, RLD }`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 22),
		Definitions: []ast.Node{
			ast.NewEnumDefinition(&ast.EnumDefinition{
				Loc: testLoc(0, 22),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(5, 10),
				}),
				Values: []*ast.EnumValueDefinition{
					ast.NewEnumValueDefinition(&ast.EnumValueDefinition{
						Name: ast.NewName(&ast.Name{
							Value: "WO",
							Loc:   testLoc(13, 15),
						}),
						Loc: testLoc(13, 15),
					}),
					ast.NewEnumValueDefinition(&ast.EnumValueDefinition{
						Name: ast.NewName(&ast.Name{
							Value: "RLD",
							Loc:   testLoc(17, 20),
						}),
						Loc: testLoc(17, 20),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleInterface(t *testing.T) {
	body := `
interface Hello {
  world: String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 36),
		Definitions: []ast.Node{
			ast.NewInterfaceDefinition(&ast.InterfaceDefinition{
				Loc: testLoc(1, 36),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(11, 16),
				}),
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(21, 34),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(21, 26),
						}),
						Arguments: []*ast.InputValueDefinition{},
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(28, 34),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(28, 34),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleFieldWithArg(t *testing.T) {
	body := `
type Hello {
  world(flag: Boolean): String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 46),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(1, 46),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Interfaces: []*ast.Named{},
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(16, 44),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(16, 21),
						}),
						Arguments: []*ast.InputValueDefinition{
							ast.NewInputValueDefinition(&ast.InputValueDefinition{
								Loc: testLoc(22, 35),
								Name: ast.NewName(&ast.Name{
									Value: "flag",
									Loc:   testLoc(22, 26),
								}),
								Type: ast.NewNamed(&ast.Named{
									Loc: testLoc(28, 35),
									Name: ast.NewName(&ast.Name{
										Value: "Boolean",
										Loc:   testLoc(28, 35),
									}),
								}),
								DefaultValue: nil,
							}),
						},
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(38, 44),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(38, 44),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleFieldWithArgWithDefaultValue(t *testing.T) {
	body := `
type Hello {
  world(flag: Boolean = true): String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 53),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(1, 53),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Interfaces: []*ast.Named{},
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(16, 51),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(16, 21),
						}),
						Arguments: []*ast.InputValueDefinition{
							ast.NewInputValueDefinition(&ast.InputValueDefinition{
								Loc: testLoc(22, 42),
								Name: ast.NewName(&ast.Name{
									Value: "flag",
									Loc:   testLoc(22, 26),
								}),
								Type: ast.NewNamed(&ast.Named{
									Loc: testLoc(28, 35),
									Name: ast.NewName(&ast.Name{
										Value: "Boolean",
										Loc:   testLoc(28, 35),
									}),
								}),
								DefaultValue: ast.NewBooleanValue(&ast.BooleanValue{
									Value: true,
									Loc:   testLoc(38, 42),
								}),
							}),
						},
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(45, 51),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(45, 51),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleFieldWithListArg(t *testing.T) {
	body := `
type Hello {
  world(things: [String]): String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 49),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(1, 49),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Interfaces: []*ast.Named{},
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(16, 47),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(16, 21),
						}),
						Arguments: []*ast.InputValueDefinition{
							ast.NewInputValueDefinition(&ast.InputValueDefinition{
								Loc: testLoc(22, 38),
								Name: ast.NewName(&ast.Name{
									Value: "things",
									Loc:   testLoc(22, 28),
								}),
								Type: ast.NewList(&ast.List{
									Loc: testLoc(30, 38),
									Type: ast.NewNamed(&ast.Named{
										Loc: testLoc(31, 37),
										Name: ast.NewName(&ast.Name{
											Value: "String",
											Loc:   testLoc(31, 37),
										}),
									}),
								}),
								DefaultValue: nil,
							}),
						},
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(41, 47),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(41, 47),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleFieldWithTwoArg(t *testing.T) {
	body := `
type Hello {
  world(argOne: Boolean, argTwo: Int): String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 61),
		Definitions: []ast.Node{
			ast.NewObjectDefinition(&ast.ObjectDefinition{
				Loc: testLoc(1, 61),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Interfaces: []*ast.Named{},
				Fields: []*ast.FieldDefinition{
					ast.NewFieldDefinition(&ast.FieldDefinition{
						Loc: testLoc(16, 59),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(16, 21),
						}),
						Arguments: []*ast.InputValueDefinition{
							ast.NewInputValueDefinition(&ast.InputValueDefinition{
								Loc: testLoc(22, 37),
								Name: ast.NewName(&ast.Name{
									Value: "argOne",
									Loc:   testLoc(22, 28),
								}),
								Type: ast.NewNamed(&ast.Named{
									Loc: testLoc(30, 37),
									Name: ast.NewName(&ast.Name{
										Value: "Boolean",
										Loc:   testLoc(30, 37),
									}),
								}),
								DefaultValue: nil,
							}),
							ast.NewInputValueDefinition(&ast.InputValueDefinition{
								Loc: testLoc(39, 50),
								Name: ast.NewName(&ast.Name{
									Value: "argTwo",
									Loc:   testLoc(39, 45),
								}),
								Type: ast.NewNamed(&ast.Named{
									Loc: testLoc(47, 50),
									Name: ast.NewName(&ast.Name{
										Value: "Int",
										Loc:   testLoc(47, 50),
									}),
								}),
								DefaultValue: nil,
							}),
						},
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(53, 59),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(53, 59),
							}),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleUnion(t *testing.T) {
	body := `union Hello = World`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 19),
		Definitions: []ast.Node{
			ast.NewUnionDefinition(&ast.UnionDefinition{
				Loc: testLoc(0, 19),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Types: []*ast.Named{
					ast.NewNamed(&ast.Named{
						Loc: testLoc(14, 19),
						Name: ast.NewName(&ast.Name{
							Value: "World",
							Loc:   testLoc(14, 19),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_UnionWithTwoTypes(t *testing.T) {
	body := `union Hello = Wo | Rld`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 22),
		Definitions: []ast.Node{
			ast.NewUnionDefinition(&ast.UnionDefinition{
				Loc: testLoc(0, 22),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(6, 11),
				}),
				Types: []*ast.Named{
					ast.NewNamed(&ast.Named{
						Loc: testLoc(14, 16),
						Name: ast.NewName(&ast.Name{
							Value: "Wo",
							Loc:   testLoc(14, 16),
						}),
					}),
					ast.NewNamed(&ast.Named{
						Loc: testLoc(19, 22),
						Name: ast.NewName(&ast.Name{
							Value: "Rld",
							Loc:   testLoc(19, 22),
						}),
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_Scalar(t *testing.T) {
	body := `scalar Hello`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(0, 12),
		Definitions: []ast.Node{
			ast.NewScalarDefinition(&ast.ScalarDefinition{
				Loc: testLoc(0, 12),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(7, 12),
				}),
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleInputObject(t *testing.T) {
	body := `
input Hello {
  world: String
}`
	astDoc := parse(t, body)
	expected := ast.NewDocument(&ast.Document{
		Loc: testLoc(1, 32),
		Definitions: []ast.Node{
			ast.NewInputObjectDefinition(&ast.InputObjectDefinition{
				Loc: testLoc(1, 32),
				Name: ast.NewName(&ast.Name{
					Value: "Hello",
					Loc:   testLoc(7, 12),
				}),
				Fields: []*ast.InputValueDefinition{
					ast.NewInputValueDefinition(&ast.InputValueDefinition{
						Loc: testLoc(17, 30),
						Name: ast.NewName(&ast.Name{
							Value: "world",
							Loc:   testLoc(17, 22),
						}),
						Type: ast.NewNamed(&ast.Named{
							Loc: testLoc(24, 30),
							Name: ast.NewName(&ast.Name{
								Value: "String",
								Loc:   testLoc(24, 30),
							}),
						}),
						DefaultValue: nil,
					}),
				},
			}),
		},
	})
	if !reflect.DeepEqual(astDoc, expected) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expected, astDoc)
	}
}

func TestSchemaParser_SimpleInputObjectWithArgsShouldFail(t *testing.T) {
	body := `
input Hello {
  world(foo: Int): String
}`

	_, err := Parse(ParseParams{
		Source: body,
		Options: ParseOptions{
			NoLocation: false,
			NoSource:   true,
		},
	})

	expectedError := &gqlerrors.Error{
		Message: `Syntax Error GraphQL (3:8) Expected :, found (

2: input Hello {
3:   world(foo: Int): String
          ^
4: }
`,
		Stack: `Syntax Error GraphQL (3:8) Expected :, found (

2: input Hello {
3:   world(foo: Int): String
          ^
4: }
`,
		Nodes: []ast.Node{},
		Source: &source.Source{
			Body: []byte(`
input Hello {
  world(foo: Int): String
}`),
			Name: "GraphQL",
		},
		Positions: []int{22},
		Locations: []location.SourceLocation{
			{Line: 3, Column: 8},
		},
	}
	if err == nil {
		t.Fatalf("expected error, expected: %v, got: %v", expectedError, nil)
	}
	if !reflect.DeepEqual(expectedError, err) {
		t.Fatalf("unexpected document, expected: %v, got: %v", expectedError, err)
	}
}
