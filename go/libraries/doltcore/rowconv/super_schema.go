package rowconv

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// RowConvForSuperSchema creates a RowConverter for transforming rows with the the given schema to the given super schema.
// This is done by mapping the column tag and type to the super schema column representing that tag and type.
func RowConvForSuperSchema(sch, super schema.Schema) (*RowConverter, error) {
	inNameToOutName := make(map[string]string)
	allCols := sch.GetAllCols()
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		inNameToOutName[col.Name] = fmt.Sprintf("%d_%s", col.Tag, col.Kind.String())
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	fm, err := NewFieldMappingFromNameMap(sch, super, inNameToOutName)

	if err != nil {
		return nil, err
	}

	return NewRowConverter(fm)
}

// TagKindPair is a simple tuple that holds a tag and a NomsKind of a column
type TagKindPair struct {
	// Tag is the tag of a column
	Tag uint64

	// Kind is the NomsKind of a colum
	Kind types.NomsKind
}

// NameKindPair is a simple tuple that holds the name of a column and it's NomsKind
type NameKindPair struct {
	// Name is the name of the column
	Name string

	// Kind is the NomsKind of the column
	Kind types.NomsKind
}

// SuperSchemaGen is a utility class used to generate the superset of several schemas.
type SuperSchemaGen struct {
	tagKindToDestTag map[TagKindPair]uint64
	usedTags         map[uint64]struct{}
}

// NewSuperSchemaGen creates a new SuperSchemaGen
func NewSuperSchemaGen() *SuperSchemaGen {
	return &SuperSchemaGen{make(map[TagKindPair]uint64), make(map[uint64]struct{})}
}

// AddSchema will add a schema which will be incorporated into the superset of schemas
func (ss *SuperSchemaGen) AddSchema(sch schema.Schema) error {
	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tagKind := TagKindPair{Tag: tag, Kind: col.Kind}
		_, exists := ss.tagKindToDestTag[tagKind]

		if !exists {
			destTag := tag

			for {
				_, collides := ss.usedTags[destTag]
				if !collides {
					ss.tagKindToDestTag[tagKind] = destTag
					ss.usedTags[destTag] = struct{}{}
					return false, nil
				}

				if destTag == tag {
					destTag = schema.ReservedTagMin
				} else {
					destTag++
				}
			}
		}

		return false, nil
	})

	if err != nil {
		return err
	}

	return nil
}

// GenerateSuperSchema takes all the accumulated schemas and generates a schema which is the superset of all of
// those schemas.
func (ss *SuperSchemaGen) GenerateSuperSchema(additionalCols ...NameKindPair) (schema.Schema, error) {
	colColl, _ := schema.NewColCollection()
	for tagKind, destTag := range ss.tagKindToDestTag {
		colName := fmt.Sprintf("%d_%s", tagKind.Tag, tagKind.Kind.String())
		col := schema.NewColumn(colName, destTag, tagKind.Kind, false)

		var err error
		colColl, err = colColl.Append(col)

		if err != nil {
			return nil, err
		}
	}

	if len(additionalCols) > 0 {
		nextReserved := schema.ReservedTagMin
		for {
			if _, ok := ss.usedTags[nextReserved]; !ok {
				break
			}
			nextReserved++
		}

		for _, nameKindPair := range additionalCols {
			var err error
			colColl, err = colColl.Append(schema.NewColumn(nameKindPair.Name, nextReserved, nameKindPair.Kind, false))

			if err != nil {
				return nil, err
			}

			nextReserved++
		}
	}

	return schema.UnkeyedSchemaFromCols(colColl), nil
}

// AddHistoryOfTableAtCommit will traverse a commit graph adding all versions of a tables schema to the schemas being
// supersetted.
func (ss *SuperSchemaGen) AddHistoryOfTableAtCommit(ctx context.Context, tblName string, ddb *doltdb.DoltDB, cm *doltdb.Commit) error {
	addedSchemas := make(map[hash.Hash]struct{})
	processedCommits := make(map[hash.Hash]struct{})
	return ss.addHistoryOfTableAtCommit(ctx, tblName, addedSchemas, processedCommits, ddb, cm)
}

func (ss *SuperSchemaGen) addHistoryOfTableAtCommit(ctx context.Context, tblName string, addedSchemas, processedCommits map[hash.Hash]struct{}, ddb *doltdb.DoltDB, cm *doltdb.Commit) error {
	cmHash, err := cm.HashOf()

	if err != nil {
		return err
	}

	if _, ok := processedCommits[cmHash]; ok {
		return nil
	}

	processedCommits[cmHash] = struct{}{}

	root, err := cm.GetRootValue()

	if err != nil {
		return err
	}

	tbl, ok, err := root.GetTable(ctx, tblName)

	if err != nil {
		return err
	}

	if ok {
		schRef, err := tbl.GetSchemaRef()

		if err != nil {
			return err
		}

		h := schRef.TargetHash()

		if _, ok = addedSchemas[h]; !ok {
			sch, err := tbl.GetSchema(ctx)

			if err != nil {
				return err
			}

			err = ss.AddSchema(sch)

			if err != nil {
				return err
			}
		}
	}

	numParents, err := cm.NumParents()

	if err != nil {
		return err
	}

	for i := 0; i < numParents; i++ {
		cm, err := ddb.ResolveParent(ctx, cm, i)

		if err != nil {
			return err
		}

		err = ss.addHistoryOfTableAtCommit(ctx, tblName, addedSchemas, processedCommits, ddb, cm)

		if err != nil {
			return err
		}
	}

	return nil
}

// AddHistoryOfTable will traverse all commit graphs which have local branches associated with them and add all
// passed versions of a table's schema to the schemas being supersetted
func (ss *SuperSchemaGen) AddHistoryOfTable(ctx context.Context, tblName string, ddb *doltdb.DoltDB) error {
	refs, err := ddb.GetRefs(ctx)

	if err != nil {
		return err
	}

	addedSchemas := make(map[hash.Hash]struct{})
	processedCommits := make(map[hash.Hash]struct{})

	for _, ref := range refs {
		cs, err := doltdb.NewCommitSpec("HEAD", ref.String())

		if err != nil {
			return err
		}

		cm, err := ddb.Resolve(ctx, cs)

		if err != nil {
			return err
		}

		err = ss.addHistoryOfTableAtCommit(ctx, tblName, addedSchemas, processedCommits, ddb, cm)

		if err != nil {
			return err
		}
	}

	return nil
}
