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

func ScanContentTextTypeDetail(row *sql.Rows) (*Content, *locations.Address, error) {
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

const CommonContentTypeTextFields = `e.pub_id, e.last_updated, c.title, c.summary, c.slug, c.type, c.format, c.text, c.extern_path, c.last_sync, c.version_cookie, p.pub_id, p.display_name, pc.role, pc.summary_credit_order `
const CommonContentTypeTextFrom = `FROM content c JOIN contributors pc ON c.id=pc.content JOIN persons p ON cp.id=p.id `

const createContentStatement = `INSERT INTO content (id, extern_path, slug, type, title, summary, version_cookie) VALUES(?,?,?,?,?,?,?)`
const createContentTypeTextStatement = `INSERT INTO content_type_tex (id, format, text) VALUES(?,?,?)`
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

  newContent, err := SyncContent(c, ctx)
  if err != nil {
    return nil, rest.ServerError("Record created, but could not perform initial sync with external resource.", err)
  }

  return newContent, nil
}

const CommonContentGet string = `SELECT ` + CommonContentFields + `, c.id, loc.id, ea.idx, ea.label, loc.address1, loc.address2, loc.city, loc.state, loc.zip, loc.lat, loc.lng ` + CommonContentFrom + ` LEFT JOIN entity_addresses ea ON c.id=ea.entity_id AND ea.idx >= 0 LEFT JOIN locations loc ON ea.location_id=loc.id `
const getContentStatement string = CommonContentGet + `WHERE e.pub_id=? `

// GetContent retrieves a Content from a public ID string (UUID). Attempting to
// retrieve a non-existent Content results in a rest.NotFoundError. This is used
// primarily to retrieve a Content in response to an API request.
//
// Consider using GetContentByID to retrieve a Content from another backend/DB
// function. TODO: reference discussion of internal vs public IDs.
func GetContent(pubId string, ctx context.Context) (*Content, rest.RestError) {
  return getContentHelper(getContentQuery, pubId, ctx, nil)
}

// GetContentInTxn retrieves a Content by public ID string (UUID) in the context
// of an existing transaction. See GetContent.
func GetContentInTxn(pubId string, ctx context.Context, txn *sql.Tx) (*Content, rest.RestError) {
  return getContentHelper(getContentQuery, pubId, ctx, txn)
}

const getContentByAuthIdStatement string = CommonContentGet + ` WHERE u.auth_id=? `
// GetContentByAuthId retrieves a Content from a public authentication ID string
// provided by the authentication provider (firebase). Attempting to retrieve a
// non-existent Content results in a rest.NotFoundError. This is used primarily
// to retrieve a Content in response to an API request, especially
// '/content/self'.
func GetContentByAuthId(authId string, ctx context.Context) (*Content, rest.RestError) {
  return getContentHelper(getContentByAuthIdQuery, authId, ctx, nil)
}

// GetContentByAuthIdInTxn retrieves a Content by public authentication ID string
// in the context of an existing transaction. See GetContentByAuthId.
func GetContentByAuthIdInTxn(authId string, ctx context.Context, txn *sql.Tx) (*Content, rest.RestError) {
  return getContentHelper(getContentByAuthIdQuery, authId, ctx, txn)
}

const getContentByIdStatement string = CommonContentGet + ` WHERE c.id=? `
// GetContentByID retrieves a Content by internal ID. As the internal ID must
// never be exposed to users, this method is exclusively for internal/backend
// use. Specifically, since Content are associated with other Entities through
// the internal ID (i.e., foreign keys use the internal ID), this function is
// most often used to retrieve a Content which is to be bundled in a response.
//
// Use GetContent to retrieve a Content in response to an API request. TODO:
// reference discussion of internal vs public IDs.
func GetContentByID(id int64, ctx context.Context) (*Content, rest.RestError) {
  return getContentHelper(getContentByIdQuery, id, ctx, nil)
}

// GetContentByIDInTxn retrieves a Content by internal ID in the context of an
// existing transaction. See GetContentByID.
func GetContentByIDInTxn(id int64, ctx context.Context, txn *sql.Tx) (*Content, rest.RestError) {
  return getContentHelper(getContentByIdQuery, id, ctx, txn)
}

func getContentHelper(stmt *sql.Stmt, id interface{}, ctx context.Context, txn *sql.Tx) (*Content, rest.RestError) {
  if txn != nil {
    stmt = txn.Stmt(stmt)
  }
	rows, err := stmt.QueryContext(ctx, id)
	if err != nil {
		return nil, rest.ServerError("Error retrieving content.", err)
	}
	defer rows.Close()

	var content *Content
  var address *locations.Address
  var addresses locations.Addresses = make(locations.Addresses, 0)
	for rows.Next() {
    var err error
    // The way the scanner works, it processes all the data each time. :(
    // 'content' gets updated with an equivalent structure while we gather up
    // the addresses.
    if content, address, err = ScanContentDetail(rows); err != nil {
      return nil, rest.ServerError(fmt.Sprintf("Problem getting data for content: '%v'", id), err)
    }

    if address.LocationId.Valid {
	    addresses = append(addresses, address)
    }
	}
  if content != nil {
    content.Addresses = addresses
    content.FormatOut()
  } else {
    return nil, rest.NotFoundError(fmt.Sprintf(`Content '%s' not found.`, id), nil)
  }

	return content, nil
}

// BUG(zane@liquid-labs.com): UpdateContent should use internal IDs if available
// on the Content struct. (I'm assuming this is slightly more efficient, though
// we should test.)

// UpdatesContent updates the canonical Content record. Attempting to update a
// non-existent Content results in a rest.NotFoundError.
func UpdateContent(p *Content, ctx context.Context) (*Content, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newP, restErr := UpdateContentInTxn(p, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if restErr == nil {
    defer txn.Commit()
  }

  return newP, restErr
}

// UpdatesContentInTxn updates the canonical Content record within an existing
// transaction. See UpdateContent.
func UpdateContentInTxn(p *Content, ctx context.Context, txn *sql.Tx) (*Content, rest.RestError) {
  if c.Addresses != nil {
    c.Addresses.CompleteAddresses(ctx)
  }
  var err error
  if c.Addresses != nil {
    if restErr := c.Addresses.Update(c.PubId.String, ctx, txn); restErr != nil {
      defer txn.Rollback()
      // TODO: this message could be misleading; like the content was updated, and just the addresses not
      return nil, restErr
    }
  }

  var updateStmt *sql.Stmt = txn.Stmt(updateContentQuery)
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

// TODO: enable update of AuthID
const updateContentStatement = `UPDATE content c JOIN users u ON u.id=c.id JOIN entities e ON c.id=e.id SET u.active=?, u.legal_id=?, u.legal_id_type=?, c.display_name=?, c.phone=?, c.email=?, c.phone_backup=?, e.last_updated=0 WHERE e.pub_id=?`
var createContentQuery, updateContentQuery, getContentQuery, getContentByAuthIdQuery, getContentByIdQuery *sql.Stmt
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
  if getContentByIdQuery, err = db.Prepare(getContentByIdStatement); err != nil {
    log.Fatalf("mysql: prepare get content by ID stmt: %v", err)
  }
  if updateContentQuery, err = db.Prepare(updateContentStatement); err != nil {
    log.Fatalf("mysql: prepare update content stmt: %v", err)
  }
}
