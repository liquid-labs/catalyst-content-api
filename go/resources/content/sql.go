package content

import (
  "context"
  "database/sql"
  "fmt"
  "log"
  "strconv"

  "github.com/Liquid-Labs/go-api/sqldb"
  "github.com/Liquid-Labs/go-nullable-mysql/nulls"
  "github.com/Liquid-Labs/go-rest/rest"
  "github.com/Liquid-Labs/catalyst-core-api/go/resources/users"
  "github.com/Liquid-Labs/catalyst-core-api/go/resources/locations"
)

var ContentSorts = map[string]string{
  "": `c.content ASC `,
  `title-asc`: `c.content ASC `,
  `title-desc`: `c.content DESC `,
}

func ScanContentSummary(row *sql.Rows) (*ContentSummary, *ContributorSummary, error) {
	var c ContentSummary
  var p ContributorSummary

	if err := row.Scan(&c.PubId, &c.LastUpdated, &c.Title, &c.Summary, &c.Slug, &c.Type,
      /* limited person data */ &p.PubId, &p.DisplayName,
      /* contrib specific data */ &p.Role, &p.SummaryCreditOrder); err != nil {
		return nil, err
	}

	return &c, &p, nil
}

func ScanContentTypeTextDetail(row *sql.Rows) (*Content, *locations.Address, error) {
  var c Content
  var p ContributorSummary

	if err := row.Scan(&c.PubId, &c.LastUpdated, &c.Title, &c.Summary, &c.Slug, &c.Type,
      &c.Format, &c.Text, &c.ExternPath, &c.LastSync, &c.VersionCookie,
      /* limited person data */ &p.PubId, &p.DisplayName,
      /* contrib specific data */ &p.Role, &p.SummaryCreditOrder); err != nil {); err != nil {
		return nil, err
	}

	return &c, &p, nil
}

// implement rest.ResultBuilder
func BuildContentResults(rows *sql.Rows) (interface{}, error) {
  results := make([]*ContentSummary, 0)
  for rows.Next() {
    content, err := ScanContentSummary(rows)
    if err != nil {
      return nil, err
    }

    results = append(results, content)
  }

  return results, nil
}

// Implements rest.GeneralSearchWhereBit
func ContentGeneralWhereGenerator(term string, params []interface{}) (string, []interface{}, error) {
  likeTerm := `%`+term+`%`
  whereBit := "AND (c.title LIKE ? OR p.display_name LIKE ?) "
  params = append(params, likeTerm, likeTerm)

  return whereBit, params, nil
}

const CommonContentFields = `e.pub_id, e.last_updated, c.title, c.summary, c.slug, c.type `
const CommonCententTypeTextFields = `ctt.format, ctt.text, c.extern_path, c.last_sync, c.version_cookie `
const CommonContentContribFields = `p.pub_id, p.display_name, pc.role, pc.summary_credit_order `
const CommonContentFrom = `FROM content c JOIN contributors pc ON c.id=pc.content JOIN persons p ON cp.id=p.id `
const CommonContentTypeTextFrom = `JOIN ctt content_type_text ON c.id=ctt.id `

const createContentStatement = `INSERT INTO content (id, extern_path, slug, type, title, summary, version_cookie) VALUES(?,?,?,?,?,?,?)`
const createContentTypeTextStatement = `INSERT INTO content_type_text (id, format, text) VALUES(?,?,?)`
func CreateTypeTextContent(c *ContentTypeText, ctx context.Context) (*ContentText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not create content record. (txn error)", err)
  }
  newP, restErr := CreateContentTypeTextInTxn(p, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if err == nil {
    defer txn.Commit()
  }
  return newP, restErr
}

func CreateContentTypeTextInTxn(c *ContentTypeText, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  var err error
  newId, restErr := users.createContentInTxn(c.ContentSummary, txn)
  if restErr != nil {
    defer txn.Rollback()
		return nil, restErr
  }

  c.Id = nulls.NewInt64(newId)

	_, err = txn.Stmt(createContentTypeTextQuery).Exec(newId, c.Format, c.Text)
	if err != nil {
    // TODO: can we do more to tell the cause of the failure? We assume it's due to malformed data with the HTTP code
    defer txn.Rollback()
    log.Print(err)
		return nil, rest.UnprocessableEntityError("Failure creating content.", err)
	}

  if restErr := LinkContributors(c, ctx, txn); restErr != nil {
    defer txn.Rollback()
    return nil, restErr
  }

  defer txn.Commit()

  newContent, err := SyncContentTypeText(c, ctx)
  if err != nil {
    return nil, rest.ServerError("Record created, but could not perform initial sync with external resource.", err)
  }

  return newContent, nil
}

const CommonContentTypeTextGet string = `SELECT ` + CommonContentFields + CommonContentTypeTextFields + CommonContentFrom + CommonContentTypeTextFrom
const getContentTypeTextStatement string = CommonContentTypeTextGet + `WHERE e.pub_id=? `

// GetContentTypeText retrieves a ContentTypeText from a public ID string
// (UUID). Attempting to  retrieve a non-existent item results in a
// rest.NotFoundError. This is used primarily to retrieve an item in response to
// an API request.
//
// Consider using GetContentTypeTextByID to retrieve a ContentTypeText from
// another backend/DB function. TODO: reference discussion of internal vs public
// IDs.
func GetContentTypeText(pubID string, ctx context.Context) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextQuery, ctx, nil, pubID)
}

// GetContentTypeTextInTxn retrieves a ContentTypeTex by public ID string (UUID)
//  in the context of an existing transaction. See GetContentTypeText.
func GetContentTypeTextInTxn(pubID string, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextQuery, ctx, txn, pubID)
}

const getContentTypeTextByNSSlugStatement string = CommonContentTypeTextGet + ` WHERE c.namespace=? AND c.slug=? `
// GetContentTypeTextByNSSlug retrieves a ContentTypeText from a content
// namespace and slug. Attempting to retrieve a non-existent item results in a
// rest.NotFoundError. This is used primarily to retrieve an item in response to
// an API request.
func GetContentTypeTextByNSSlug(namespace string, slug string, ctx context.Context) (*ContentTypeText, rest.RestError) {
  return getContentHelper(getContentByNSSlugQuery, ctx, nil, namespace, slug)
}

// GetContentTypeTextByNSSlugInTxn retrieves a ContentTypeText by a namespace
// and slug in the context of an existing transaction. See
// GetContentTypeTextByNSSlug.
func GetContentTypeTextByNSSlugInTxn(namespace string, slug string, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  return getContentHelper(getContentByAuthIdQuery, ctx, txn, namespace, slug)
}

const getContentTypeTextByIDStatement string = CommonContentTypeTextGet + ` WHERE c.id=? `
// GetContentTypeTextByID retrieves a ContentTypeText by internal ID. As the
// internal ID must never be exposed to users, this method is exclusively for
// internal/backend use. Specifically, since ContentTypeText are associated with
// other Entities through the internal ID (i.e., foreign keys use the internal
// ID), this function is most often used to retrieve a ContentTypeText which is
// to be bundled in a response.
//
// Use GetContentTypeText to retrieve a ContentTypeText in response to an API
// request. TODO: reference discussion of internal vs public IDs.
func GetContentTypeTextByID(id int64, ctx context.Context) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextByIDQuery, ctx, nil, id)
}

// GetContentByIDInTxn retrieves a Content by internal ID in the context of an
// existing transaction. See GetContentByID.
func GetContentByIDInTxn(id int64, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentByIDQuery, ctx, txn, id)
}

func getContentTypeTextHelper(stmt *sql.Stmt, ctx context.Context, txn *sql.Tx, ids ...interface{}) (*ContentTypeText, rest.RestError) {
  if txn != nil {
    stmt = txn.Stmt(stmt)
  }
	rows, err := stmt.QueryContext(ctx, ids...)
	if err != nil {
		return nil, rest.ServerError("Error retrieving content.", err)
	}
	defer rows.Close()

	var content *ContentTypeText
  var contributor ContributorSummary
  var contributors ContributorSummaries = make(ContributorSummaries, 0)
	for rows.Next() {
    var err error
    // The way the scanner works, it processes all the data each time. :(
    // 'content' gets updated with an equivalent structure while we gather up
    // the contributors.
    if content, contributor, err = ScanContentDetail(rows); err != nil {
      return nil, rest.ServerError(fmt.Sprintf("Problem getting data for content: '%v'", id), err)
    }

    contributors = append(contributors, contributor)
	}
  if content != nil {
    content.Contributors = contributors
  } else {
    return nil, rest.NotFoundError(fmt.Sprintf(`Content '%s' not found.`, id), nil)
  }

	return content, nil
}

const updateContentStatement = `UPDATE content c JOIN content_type_text ctt ON c.id=ctt.id JOIN entities e ON c.id=e.id SET e.last_updated=0, c.extern_path=?, c.namespace, c.slug, WHERE e.pub_id=?`

// UpdatesTypeTextContent updates the ContentTextType excepting the Type and
// Contributors. Note the following caveats:
// * Contritbutors are udpated separately for efficiency via
//   UpdateContentContributors.
// * Type cannot be updated. Attempting to change Type will cause an error, and
//   the UI data model should not allow such changes in the first insance.
//
// Attempting to update a non-existent ContentTypeText
// results in a rest.NotFoundError.
func UpdateTypeTextContent(c *ContentTypeText, ctx context.Context) (*ContentTypeText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newC, restErr := UpdateContentInTxn(c, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if restErr == nil {
    defer txn.Commit()
  }

  return newC, restErr
}

// UpdatesContentTypeTextInTxn updates the ContentTypeText record within an existing
// transaction. See UpdateContentTypeText.
func UpdateContentTypeTextInTxn(c *Content, ctx context.Context, txn *sql.Tx) (*Content, rest.RestError) {
  var updateStmt *sql.Stmt = txn.Stmt(updateContentTypeTextQuery)
  _, err = updateStmt.Exec(c.Active, c.LegalID, c.LegalIDType, c.DisplayName, c.Phone, c.Email, c.PhoneBackup, c.PubId)
  if err != nil {
    if txn != nil {
      defer txn.Rollback()
    }
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newContent, err := GetContentInTxn(c.PubId.String, ctx, txn)
  if err != nil {
    return nil, rest.ServerError("Problem retrieving newly updated content.", err)
  }
  // Carry any 'ChangeDesc' made by the geocoding out.
  c.PromoteChanges()
  newContent.ChangeDesc = c.ChangeDesc

  return newContent, nil
}

func UpdateContentContributors(c *Content, ctx context.Context, txn *sql.Tx) *Context, rest.RestError {
  foo()
}

// TODO: enable update of AuthID
var createContentQuery, updateContentQuery, getContentQuery, getContentByAuthIdQuery, getContentByIDQuery *sql.Stmt
func SetupDB(db *sql.DB) {
  var err error
  if createContentQuery, err = db.Prepare(createContentStatement); err != nil {
    log.Fatalf("mysql: prepare create content stmt:\n%v\n%s", err, createContentStatement)
  }
  if getContentQuery, err = db.Prepare(getContentStatement); err != nil {
    log.Fatalf("mysql: prepare get content stmt: %v", err)
  }
  if getContentByAuthIdQuery, err = db.Prepare(getContentByAuthIdStatement); err != nil {
    log.Fatalf("mysql: prepare get content by auth ID stmt: %v", err)
  }
  if getContentByIDQuery, err = db.Prepare(getContentByIDStatement); err != nil {
    log.Fatalf("mysql: prepare get content by ID stmt: %v", err)
  }
  if updateContentQuery, err = db.Prepare(updateContentStatement); err != nil {
    log.Fatalf("mysql: prepare update content stmt: %v", err)
  }
}
