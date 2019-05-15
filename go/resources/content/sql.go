package content

import (
  "context"
  "database/sql"
  "fmt"

  "github.com/Liquid-Labs/go-api/sqldb"
  "github.com/Liquid-Labs/go-nullable-mysql/nulls"
  "github.com/Liquid-Labs/go-rest/rest"
  "github.com/Liquid-Labs/catalyst-core-api/go/resources/entities"

  model "github.com/Liquid-Labs/catalyst-content-model/go/resources/content"
)

var ContentSorts = map[string]string{
  "": `c.content ASC `,
  `title-asc`: `c.content ASC `,
  `title-desc`: `c.content DESC `,
}

func scanContentSummary(row *sql.Rows) (*model.ContentSummary, *model.ContributorSummary, error) {
	var c model.ContentSummary
  var p model.ContributorSummary

	if err := row.Scan(&c.PubId, &c.LastUpdated, &c.Title, &c.Summary, &c.Namespace, &c.Slug, &c.Type,
      /* limited person data */ &p.PubId, &p.DisplayName,
      /* contrib specific data */ &p.Role, &p.SummaryCreditOrder); err != nil {
		return nil, nil, err
	}

	return &c, &p, nil
}

func scanContentTypeTextDetail(row *sql.Rows) (*model.ContentTypeText, *model.ContributorSummary, error) {
  var c model.ContentTypeText
  var p model.ContributorSummary

	if err := row.Scan(&c.PubId, &c.LastUpdated, &c.Title, &c.Summary, &c.Namespace, &c.Slug, &c.Type,
      &c.ExternPath, &c.LastSync, &c.VersionCookie, &c.Format, &c.Text,
      /* limited person data */ &p.PubId, &p.DisplayName,
      /* contrib specific data */ &p.Role, &p.SummaryCreditOrder); err != nil {
		return nil, nil, err
	}

	return &c, &p, nil
}

// implement rest.ResultBuilder
func BuildContentResults(rows *sql.Rows) (interface{}, error) {
  var lastID int64 = 0
  results := make([]*model.ContentSummary, 0)
  contributors := make([]*model.ContributorSummary, 0)
  var content *model.ContentSummary
  for rows.Next() {
    content, contributor, err := scanContentSummary(rows)
    if err != nil {
      return nil, err
    }

    contributors = append(contributors, contributor)
    if lastID != content.Id.Int64 {
      content.Contributors = contributors
      results = append(results, content)
      contributors = make([]*model.ContributorSummary, 0)
    }
  }

  if content != nil {
    content.Contributors = contributors
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

func CreateContentTypeText(c *model.ContentTypeText, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
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

func createContentSummaryInTxn(c *model.ContentSummary, txn *sql.Tx) (int64, rest.RestError) {
  if id, restErr := entities.CreateEntityInTxn(txn); restErr != nil {
    defer txn.Rollback()
    return 0, restErr
  } else {
    createStmt := txn.Stmt(createContentStmt)
    _, err := createStmt.Exec(id, c.ExternPath, c.Slug, c.Type, c.Title, c.Summary, c.VersionCookie)
    if (err != nil) {
      defer txn.Rollback()
      return 0, rest.ServerError("Could not create content record; error creating content.", err)
    }
    return id, nil
  }
}

func CreateContentTypeTextInTxn(c *model.ContentTypeText, ctx context.Context, txn *sql.Tx) (*model.ContentTypeText, rest.RestError) {
  var err error
  newID, restErr := createContentSummaryInTxn(&c.ContentSummary, txn)
  if restErr != nil {
    // already rolled back
		return nil, restErr
  }
  c.Id = nulls.NewInt64(newID)

	_, err = txn.Stmt(createContentTypeTextStmt).Exec(newID, c.Format, c.Text)
	if err != nil {
    // TODO: can we do more to tell the cause of the failure? We assume it's due to malformed data with the HTTP code
    defer txn.Rollback()
		return nil, rest.UnprocessableEntityError("Failure creating content.", err)
	}

  contribInsStmt := txn.Stmt(contributorInsertWithContentIDStmt)
  for _, contrib := range c.Contributors {
    if _, err := contribInsStmt.ExecContext(ctx, newID, contrib.Role, contrib.SummaryCreditOrder, contrib.PubId); err != nil {
      defer txn.Rollback()
      // TODO: can we tell more about why? We're assuming bad data here.
      return nil, rest.UnprocessableEntityError("Error updating contributors. Possible bad data.", nil)
    }
  }

  defer txn.Commit()

  newContent, err := SyncContentTypeText(c, ctx)
  if err != nil {
    return nil, rest.ServerError("Record created, but could not perform initial sync with external resource.", err)
  }

  return newContent, nil
}

// GetContentTypeText retrieves a model.ContentTypeText from a public ID string
// (UUID). Attempting to  retrieve a non-existent item results in a
// rest.NotFoundError. This is used primarily to retrieve an item in response to
// an API request.
//
// Consider using GetContentTypeTextByID to retrieve a model.ContentTypeText from
// another backend/DB function. TODO: reference discussion of internal vs public
// IDs.
func GetContentTypeText(pubID string, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextStmt, ctx, nil, pubID)
}

// GetContentTypeTextInTxn retrieves a model.ContentTypeText by public ID string (UUID)
//  in the context of an existing transaction. See GetContentTypeText.
func GetContentTypeTextInTxn(pubID string, ctx context.Context, txn *sql.Tx) (*model.ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextStmt, ctx, txn, pubID)
}

// GetContentTypeTextByNSSlug retrieves a model.ContentTypeText from a content
// namespace and slug. Attempting to retrieve a non-existent item results in a
// rest.NotFoundError. This is used primarily to retrieve an item in response to
// an API request.
func GetContentTypeTextByNSSlug(namespace string, slug string, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextByNSSlugStmt, ctx, nil, namespace, slug)
}

// GetContentTypeTextByNSSlugInTxn retrieves a model.ContentTypeText by a namespace
// and slug in the context of an existing transaction. See
// GetContentTypeTextByNSSlug.
func GetContentTypeTextByNSSlugInTxn(namespace string, slug string, ctx context.Context, txn *sql.Tx) (*model.ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextByNSSlugStmt, ctx, txn, namespace, slug)
}

// GetContentTypeTextByID retrieves a model.ContentTypeText by internal ID. As the
// internal ID must never be exposed to users, this method is exclusively for
// internal/backend use. Specifically, since model.ContentTypeText are associated with
// other Entities through the internal ID (i.e., foreign keys use the internal
// ID), this function is most often used to retrieve a model.ContentTypeText which is
// to be bundled in a response.
//
// Use GetContentTypeText to retrieve a model.ContentTypeText in response to an API
// request. TODO: reference discussion of internal vs public IDs.
func GetContentTypeTextByID(id int64, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextByIDStmt, ctx, nil, id)
}

// GetContentByIDInTxn retrieves a Content by internal ID in the context of an
// existing transaction. See GetContentByID.
func GetContentTypeTextByIDInTxn(id int64, ctx context.Context, txn *sql.Tx) (*model.ContentTypeText, rest.RestError) {
  return getContentTypeTextHelper(getContentTypeTextByIDStmt, ctx, txn, id)
}

func getContentTypeTextHelper(stmt *sql.Stmt, ctx context.Context, txn *sql.Tx, ids ...interface{}) (*model.ContentTypeText, rest.RestError) {
  if txn != nil {
    stmt = txn.Stmt(stmt)
  }
	rows, err := stmt.QueryContext(ctx, ids...)
	if err != nil {
		return nil, rest.ServerError("Error retrieving content.", err)
	}
	defer rows.Close()

	var content *model.ContentTypeText
  var contributor *model.ContributorSummary
  var contributors model.ContributorSummaries = make(model.ContributorSummaries, 0)
	for rows.Next() {
    var err error
    // The way the scanner works, it processes all the data each time. :(
    // 'content' gets updated with an equivalent structure while we gather up
    // the contributors.
    if content, contributor, err = scanContentTypeTextDetail(rows); err != nil {
      return nil, rest.ServerError(fmt.Sprintf("Problem getting data for content: '%v'", ids), err)
    }

    contributors = append(contributors, contributor)
	}
  if content != nil {
    content.Contributors = contributors
  } else {
    return nil, rest.NotFoundError(fmt.Sprintf(`Content '%v' not found.`, ids), nil)
  }

	return content, nil
}

// UpdateContentTypeText updates the ContentTextType excepting the Type and
// Contributors. Note thxte following caveats:
// * Contritbutors are udpated separately for efficiency via
//   UpdateContentContributors.
// * Type cannot be updated. Attempting to change Type will cause an error, and
//   the UI data model should not allow such changes in the first insance.
// * If 'ExternPath' is non-nill and non-null/valid, then the text will be
//   updated locally. Otherwise, the text is unchanged and users will instead
//   SyncContentTypeText to update the local text.
//
// Attempting to update a non-existent model.ContentTypeText
// results in a rest.NotFoundError.
func UpdateContentTypeText(c *model.ContentTypeText, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newC, restErr := UpdateContentTypeTextInTxn(c, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if restErr == nil {
    defer txn.Commit()
  }

  return newC, restErr
}

// UpdatesContentTypeTextInTxn updates the model.ContentTypeText record within an existing
// transaction. See UpdateContentTypeText.
func UpdateContentTypeTextInTxn(c *model.ContentTypeText, ctx context.Context, txn *sql.Tx) (*model.ContentTypeText, rest.RestError) {
  var err error
  if (!c.ExternPath.IsValid()) {
    updateStmt := txn.Stmt(updateContentTypeTextWithTextStmt)
    _, err = updateStmt.Exec(c.Title, c.Summary, c.ExternPath, c.Namespace, c.Slug, c.Format, c.Text, c.PubId)
  } else {
    updateStmt := txn.Stmt(updateContentTypeTextSansTextStmt)
    _, err = updateStmt.Exec(c.Title, c.Summary, c.ExternPath, c.Namespace, c.Slug, c.Format, c.PubId)
  }
  if err != nil {
    if txn != nil {
      defer txn.Rollback()
    }
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newContent, restErr := GetContentTypeTextInTxn(c.PubId.String, ctx, txn)
  if err != nil {
    return nil, restErr
  }

  return newContent, nil
}

func UpdateContentTypeTextOnlyText(c *model.ContentTypeText, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content record.", err)
  }

  updateStmt := txn.Stmt(updateContentTypeTextOnlyTextStmt)
  _, err = updateStmt.Exec(c.Text, c.PubId)

  if err != nil {
    if txn != nil {
      defer txn.Rollback()
    }
    return nil, rest.ServerError("Could not update content record.", err)
  }

  newContent, restErr := GetContentTypeTextInTxn(c.PubId.String, ctx, txn)
  if err != nil {
    return nil, restErr
  } else {
    defer txn.Commit()
  }

  return newContent, nil
}

func UpdateContentTypeTextContributors(c *model.ContentTypeText, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  txn, err := sqldb.DB.Begin()
  if err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Could not update content contributors. (txn error)", err)
  }
  newC, restErr := UpdateContentTypeTextContributorsInTxn(c, ctx, txn)
  // txn already rolled back if in error, so we only need to commit if no error
  if err == nil {
    defer txn.Commit()
  }
  return newC, restErr
}

func UpdateContentTypeTextContributorsInTxn(c *model.ContentTypeText, ctx context.Context, txn *sql.Tx) (*model.ContentTypeText, rest.RestError) {
  delStmt := txn.Stmt(contributorsDeleteStmt)
  insStmt := txn.Stmt(contributorInsertStmt)

  if _, err := delStmt.ExecContext(ctx, c.Id); err != nil {
    defer txn.Rollback()
    return nil, rest.ServerError("Error updating contributors (clear phase).", err)
  }
  for _, contrib := range c.Contributors {
    if _, err := insStmt.ExecContext(ctx, contrib.Role, contrib.SummaryCreditOrder, contrib.PubId, c.PubId); err != nil {
      defer txn.Rollback()
      // TODO: can we tell more about why? We're assuming bad data here.
      return nil, rest.UnprocessableEntityError("Error updating contributors. Possible bad data.", err)
    }
  }

  return GetContentTypeTextInTxn(c.PubId.String, ctx, txn)
}
