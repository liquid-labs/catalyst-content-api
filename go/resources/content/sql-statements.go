package content

import (
  "database/sql"
  "log"

  queries "github.com/Liquid-Labs/catalyst-content-model/go/resources/content"
)

var createContentStmt,
  createContentTypeTextStmt,
  getContentTypeTextStmt,
  getContentTypeTextByNSSlugStmt,
  getContentTypeTextByIDStmt,
  updateContentTypeTextSansTextStmt,
  updateContentTypeTextWithTextStmt,
  updateContentTypeTextOnlyTextStmt,
  contributorsDeleteStmt,
  contributorInsertStmt,
  contributorInsertWithContentIDStmt *sql.Stmt

func SetupDB(db *sql.DB) {
  stmtMap := map[string]**sql.Stmt{
    queries.CreateContentQuery: &createContentStmt,
    queries.CreateContentTypeTextQuery: &createContentTypeTextStmt,
    queries.GetContentTypeTextQuery: &getContentTypeTextStmt,
    queries.GetContentTypeTextByNSSlugQuery: &getContentTypeTextByNSSlugStmt,
    queries.GetContentTypeTextByIDQuery: &getContentTypeTextByIDStmt,
    queries.UpdateContentTypeTextSansTextQuery: &updateContentTypeTextSansTextStmt,
    queries.UpdateContentTypeTextWithTextQuery: &updateContentTypeTextWithTextStmt,
    queries.UpdateContentTypeTextOnlyTextQuery: &updateContentTypeTextOnlyTextStmt,
    queries.ContributorsDeleteQuery: &contributorsDeleteStmt,
    queries.ContributorInsertQuery: &contributorInsertStmt,
    queries.ContributorInsertWithContentIDQuery: &contributorInsertWithContentIDStmt,
  }

  for query, permPointer := range stmtMap {
    if stmt, err := db.Prepare(query); err != nil {
      log.Fatalf("mysql: error preparing query:\n%s\nerror:\n%v", query, err)
    } else {
      *permPointer = stmt
    }
  }
}
