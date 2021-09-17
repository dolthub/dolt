package doltdb

//const BackupRemoteKey = "DOLT_BACKUP_REMOTE"
//var postCommitHooks []datas.UpdateHook
//
//func init() {
//	backupUrl := os.Getenv(BackupRemoteKey)
//	if backupUrl != "" {
//		// parse remote
//		ctx := context.Background()
//		r, srcDb, err := env.CreateRemote(ctx, "backup", backupUrl, nil, nil)
//		if err != nil {
//			return
//		}
//		// build destDB
//
//		postCommitHooks = append(postCommitHooks, func(ctx context.Context, ds datas.Dataset) error {
//			headRef, _, _ := ds.MaybeHeadRef()
//			//id := ds.ID()
//			//return backup.Backup(ctx, srcDb, "temp", r, headRef, nil, nil)
//		})
//	}
//}
