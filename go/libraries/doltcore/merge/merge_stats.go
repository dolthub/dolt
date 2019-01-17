package merge

type TableMergeOp int

const (
	TableUnmodified TableMergeOp = iota
	TableAdded
	TableRemoved
	TableModified
)

type MergeStats struct {
	Operation     TableMergeOp
	Adds          int
	Deletes       int
	Modifications int
	Conflicts     int
}
