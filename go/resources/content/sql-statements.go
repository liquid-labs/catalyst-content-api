package content

import (
  "database/sql"
  "log"
)

// create
const createContentQuery = `INSERT INTO content (id, extern_path, slug, type, title, summary, version_cookie) VALUES(?,?,?,?,?,?,?)`
const createContentTypeTextQuery = `INSERT INTO content_type_text (id, format, text) VALUES(?,?,?)`
// retrieve
const CommonContentFields = `e.pub_id, e.last_updated, c.title, c.summary, c.namespace, c.slug, c.type `
const CommonCententTypeTextFields = `ctt.format, ctt.text, c.extern_path, c.last_sync, c.version_cookie `
const CommonContentContribFields = `p.pub_id, p.display_name, pc.role, pc.summary_credit_order `
const CommonContentFrom = `FROM content c JOIN contributors pc ON c.id=pc.content JOIN persons p ON cp.id=p.id `
const CommonContentTypeTextFrom = `JOIN ctt content_type_text ON c.id=ctt.id `
const CommonContentTypeTextGet string = `SELECT ` + CommonContentFields + CommonContentTypeTextFields + CommonContentFrom + CommonContentTypeTextFrom
const getContentTypeTextQuery string = CommonContentTypeTextGet + `WHERE e.pub_id=? `
const getContentTypeTextByNSSlugQuery string = CommonContentTypeTextGet + ` WHERE c.namespace=? AND c.slug=? `
const getContentTypeTextByIDQuery string = CommonContentTypeTextGet + ` WHERE c.id=? `
// update
const updateContentQuery = `UPDATE content c JOIN content_type_text ctt ON c.id=ctt.id JOIN entities e ON c.id=e.id SET e.last_updated=0, c.title=?, c.summary=?, c.extern_path=?, c.namespace=?, c.slug=?, ctt.format=? WHERE e.pub_id=?`
const contributorsDeleteQuery = `DELETE * FROM contributors WHERE content=?`
const contributorsInsertQuery = `INSERT INTO contributors (id, content, role, summary_credit_order) SELECT persons.id, content.id, ?, ? FROM persons p JOIN content c ON p.pub_id=? AND c.pub_id=?`
const contributorInsertByContentIDQuery = `INSERT INTO contributors (id, content, role, summary_credit_order) SELECT persons.id, ?, ?, ? FROM persons p WHERE p.pub_id=?`

var createContentStmt, updateContentStmt, getContentStmt, getContentByAuthIdStmt, getContentByIDStmt *sql.Stmt

func SetupDB(db *sql.DB) {
  var err error
  if createContentStmt, err = db.Prepare(createContentQuery); err != nil {
    log.Fatalf("mysql: prepare create content stmt:\n%v\n%s", err, createContentQuery)
  }
  if getContentStmt, err = db.Prepare(getContentQuery); err != nil {
    log.Fatalf("mysql: prepare get content stmt: %v", err)
  }
  if getContentByAuthIdStmt, err = db.Prepare(getContentByAuthIdQuery); err != nil {
    log.Fatalf("mysql: prepare get content by auth ID stmt: %v", err)
  }
  if getContentByIDStmt, err = db.Prepare(getContentByIDQuery); err != nil {
    log.Fatalf("mysql: prepare get content by ID stmt: %v", err)
  }
  if updateContentStmt, err = db.Prepare(updateContentQuery); err != nil {
    log.Fatalf("mysql: prepare update content stmt: %v", err)
  }
}
