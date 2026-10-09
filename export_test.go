package litestream

// Unexported helpers exposed to the external litestream_test package.
var (
	CheckForkPlan = checkForkPlan
	ForkTargets   = forkTargets
)

// ForgetMaxLTXFileInfo drops the cached max file info for level, so the next
// lookup lists the replica.
func (db *DB) ForgetMaxLTXFileInfo(level int) {
	db.maxLTXFileInfos.Lock()
	defer db.maxLTXFileInfos.Unlock()
	delete(db.maxLTXFileInfos.m, level)
}
