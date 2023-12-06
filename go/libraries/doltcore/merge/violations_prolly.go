package merge

import (
	"context"
	"encoding/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

func NextConstraintViolation(ctx context.Context, itr prolly.ArtifactIter, kd, vd val.TupleDesc, ns tree.NodeStore) (violationType uint64, key sql.Row, value sql.Row, err error) {
	art, err := itr.Next(ctx)
	if err != nil {
		return
	}

	key = make(sql.Row, kd.Count())
	for i := 0; i < kd.Count(); i++ {
		key[i], err = index.GetField(ctx, kd, i, art.SourceKey, ns)
		if err != nil {
			return
		}
	}

	var meta prolly.ConstraintViolationMeta
	err = json.Unmarshal(art.Metadata, &meta)
	if err != nil {
		return
	}

	value = make(sql.Row, vd.Count())
	for i := 0; i < vd.Count(); i++ {
		value[i], err = index.GetField(ctx, vd, i, meta.Value, ns)
		if err != nil {
			return
		}
	}

	return MapCVType(art.ArtType), key, value, nil
}

func MapCVType(artifactType prolly.ArtifactType) (outType uint64) {
	switch artifactType {
	case prolly.ArtifactTypeForeignKeyViol:
		outType = uint64(CvType_ForeignKey)
	case prolly.ArtifactTypeUniqueKeyViol:
		outType = uint64(CvType_UniqueIndex)
	case prolly.ArtifactTypeChkConsViol:
		outType = uint64(CvType_CheckConstraint)
	case prolly.ArtifactTypeNullViol:
		outType = uint64(CvType_NotNull)
	default:
		panic("unhandled cv type")
	}
	return
}

func UnmapCVType(in CvType) (out prolly.ArtifactType) {
	switch in {
	case CvType_ForeignKey:
		out = prolly.ArtifactTypeForeignKeyViol
	case CvType_UniqueIndex:
		out = prolly.ArtifactTypeUniqueKeyViol
	case CvType_CheckConstraint:
		out = prolly.ArtifactTypeChkConsViol
	case CvType_NotNull:
		out = prolly.ArtifactTypeNullViol
	default:
		panic("unhandled cv type")
	}
	return
}
