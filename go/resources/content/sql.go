package content

import (
  "context"
  "database/sql"
  "fmt"
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

func scanContentSummary(row *sql.Rows) (*ContentSummary, *ContributorSummary, error) {
	var c ContentSummary
  var p ContributorSummary

	if err := row.Scan(&c.PubId, &c.LastUpdated, &c.Title, &c.Summary, &c.Namespace, &c.Slug, &c.Type,
      /* limited person data */ &p.PubId, &p.DisplayName,
      /* contrib specific data */ &p.Role, &p.SummaryCreditOrder); err != nil {
		return nil, err
	}

	return &c, &p, nil
}

func scanContentTypeTextDetail(row *sql.Rows) (*Content, *locations.Address, error) {
  var c Content
  var p ContributorSummary

	if err := row.Scan(&c.PubId, &c.LastUpdated, &c.Title, &c.Summary, &c.Namespace, &c.Slug, &c.Type,
      &c.ExternPath, &c.LastSync, &c.VersionCookie, &c.Format, &c.Text,
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
    content, err := scanContentSummary(rows)
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

func (c *ContentTypeText) CreateTypeTextContent(ctx context.Context) (*ContentText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not create content record. (txn error)", err)
  }
  newP, restErr := CreateContentTypeTextInTxn(c, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if err == nil {
    defer txn.Commit()
  }
  return newP, restErr
}

func (c *ContentTypeText) CreateContentTypeTextInTxn(ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  contribInsStmt = txn.Stmt(contributorInsertByID)

  var err error
  newID, restErr := users.createContentInTxn(c.ContentSummary, txn)
  if restErr != nil {
    defer txn.Rollback()
		return nil, restErr
  }

  c.Id = nulls.NewInt64(newID)

	_, err = txn.Stmt(createContentTypeTextStmt).Exec(newID, c.Format, c.Text)
	if err != nil {
    // TODO: can we do more to tell the cause of the failure? We assume it's due to malformed data with the HTTP code
    defer txn.Rollback()
		return nil, rest.UnprocessableEntityError("Failure creating content.", err)
	}

  for contrib, _ := range c.Contributors {
    if _, err := contribInsStmt.ExecContext(ctx, newID, contrib.Role, contrib.SummaryCreditOrder, contrib.PubId); err != nil {
      defer txn.Rollback()
      // TODO: can we tell more about why? We're assuming bad data here.
      return nil, rest.UnprocessableEntityError("Error updating contributors. Possible bad data.")
    }
  }

  defer txn.Commit()

  newContent, err := SyncContentTypeText(c, ctx)
  if err != nil {
    return nil, rest.ServerError("Record created, but could not perform initial sync with external resource.", err)
  }

  return newContent, nil
}

// GetContentTypeText retrieves a ContentTypeText from a public ID string
// (UUID). Attempting to  retrieve a non-existent item results in a
// rest.NotFoundError. This is used primarily to retrieve an item in response to
// an API request.
//
// Consider using GetContentTypeTextByID to retrieve a ContentTypeText from
// another backend/DB function. TODO: reference discussion of internal vs public
// IDs.
func GetContentTypeText(pubID string, ctx context.Context) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextStmt, ctx, nil, pubID)
}

// GetContentTypeTextInTxn retrieves a ContentTypeText by public ID string (UUID)
//  in the context of an existing transaction. See GetContentTypeText.
func GetContentTypeTextInTxn(pubID string, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextStmt, ctx, txn, pubID)
}

// GetContentTypeTextByNSSlug retrieves a ContentTypeText from a content
// namespace and slug. Attempting to retrieve a non-existent item results in a
// rest.NotFoundError. This is used primarily to retrieve an item in response to
// an API request.
func GetContentTypeTextByNSSlug(namespace string, slug string, ctx context.Context) (*ContentTypeText, rest.RestError) {
  return getContentHelper(getContentByNSSlugStmt, ctx, nil, namespace, slug)
}

// GetContentTypeTextByNSSlugInTxn retrieves a ContentTypeText by a namespace
// and slug in the context of an existing transaction. See
// GetContentTypeTextByNSSlug.
func GetContentTypeTextByNSSlugInTxn(namespace string, slug string, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  return getContentHelper(getContentByAuthIdStmt, ctx, txn, namespace, slug)
}

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
  return getContentTypeTextHelper(getContentTypeTextByIDStmt, ctx, nil, id)
}

// GetContentByIDInTxn retrieves a Content by internal ID in the context of an
// existing transaction. See GetContentByID.
func GetContentTypeTextByIDInTxn(id int64, ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentByIDStmt, ctx, txn, id)
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
    if content, contributor, err = scanContentDetail(rows); err != nil {
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

// UpdateContentTypeTextContent updates the ContentTextType excepting the Type and
// Contributors. Note the following caveats:
// * Contritbutors are udpated separately for efficiency via
//   UpdateContentContributors.
// * Type cannot be updated. Attempting to change Type will cause an error, and
//   the UI data model should not allow such changes in the first insance.
// * If 'ExternPath' is non-nill and non-null/valid, then the text will be
//   updated locally. Otherwise, the text is unchanged and users will instead
//   SyncContentTypeText to update the local text.
//
// Attempting to update a non-existent ContentTypeText
// results in a rest.NotFoundError.
func (c *ContentTypeText) UpdateConentTypeTextContent(ctx context.Context) (*ContentTypeText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newC, restErr := UpdateContenTypeTexttInTxn(c, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if restErr == nil {
    defer txn.Commit()
  }

  return newC, restErr
}

// UpdatesContentTypeTextInTxn updates the ContentTypeText record within an existing
// transaction. See UpdateContentTypeText.
func (c *ContentTypeText) UpdateContentTypeTextInTxn(ctx context.Context, txn *sql.Tx) (*ContentTypeText, rest.RestError) {
  var err
  if (c.ExternPath == nil || c.ExternPath.IsNull()) {
    updateStmt := txn.Stmt(updateContentTypeTextWithTextStmt)
    _, err = updateStmt.Exec(c.Title, c.Summray, c.ExternPath, c.Namespace, c.Slug, c.Format, c.Text, c.PubId)
  } else {
    updateStmt := txn.Stmt(updateContentTypeTextSansTextStmt)
    _, err = updateStmt.Exec(c.Title, c.Summray, c.ExternPath, c.Namespace, c.Slug, c.Format, c.PubId)
  }
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

  return newContent, nil
}

func (c *ContentTypeText) UpdateContentTypeTextOnlyText(ctx context.Context) (*ContentTypeText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content record.", err)
  }

  var err
  updateStmt := txn.Stmt(updateContentTypeTextOnlyTextStmt)
  _, err = updateStmt.Exec(c.Text, c.PubId)

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

  if restErr == nil {
    defer txn.Commit()
  }

  return newC, restErr
}

func (c *ContentTypeText) UpdateContentContributors(ctx context.Context) *ContentTypeText, rest.RestError {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content contributors. (txn error)", err)
  }
  newC, restErr := UpdateContentContributorsInTxn(c, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if err == nil {
    defer txn.Commit()
  }
  return newC, restErr
}

func (c *ContentSummary) UpdateContentContributorsInTxn(ctx context.Context, txn *sql.Tx) *ContentSummary, rest.RestError {
  delStmt := txn.Stmt(deleteContributorsStmt)
  insStmt := txn.Stmt(insertContributorsStmt)

  if _, err := delStmt.ExecContext(ctx, c.Id); err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Error updating contributors (clear phase).")
  }
  for contrib, _ := range c.Contributors {
    if _, err := insStmt.ExecContext(ctx, contrib.Role, contrib.SummaryCreditOrder, contrib.PubId, c.PubId); err != nil {
      defer txn.Rollback()
      // TODO: can we tell more about why? We're assuming bad data here.
      return nil, rest.UnprocessableEntityError("Error updating contributors. Possible bad data.")
    }
  }

   GetContentSummaryInTxn(c.PubId, ctx, txn)
}
