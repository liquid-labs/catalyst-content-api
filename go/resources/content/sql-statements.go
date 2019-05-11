package content

import (
  "database/sql"
  "log"
)

const CommonContentFields = `e.pub_id, e.last_updated, c.title, c.summary, c.namespace, c.slug, c.type `
const CommonCententTypeTextFields = `ctt.format, ctt.text, c.extern_path, c.last_sync, c.version_cookie `
const CommonContentContribFields = `p.pub_id, p.display_name, pc.role, pc.summary_credit_order `
const CommonContentFrom = `FROM content c JOIN contributors pc ON c.id=pc.content JOIN persons p ON cp.id=p.id `
const CommonContentTypeTextFrom = `JOIN ctt content_type_text ON c.id=ctt.id `

const createContentQuery = `INSERT INTO content (id, extern_path, slug, type, title, summary, version_cookie) VALUES(?,?,?,?,?,?,?)`
const createContentTypeTextQuery = `INSERT INTO content_type_text (id, format, text) VALUES(?,?,?)`
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
