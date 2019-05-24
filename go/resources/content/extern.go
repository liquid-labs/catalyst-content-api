package content

import (
  "context"
  "fmt"
  "io/ioutil"
  "net/http"
  "regexp"
  "strings"

  "github.com/xanzy/go-gitlab"

  "github.com/Liquid-Labs/go-api/sqldb"
  "github.com/Liquid-Labs/go-nullable-mysql/nulls"
  "github.com/Liquid-Labs/go-rest/rest"

  model "github.com/Liquid-Labs/catalyst-content-model/go/resources/content"
)

var httpRE *regexp.Regexp = regexp.MustCompile(`^https?://`)

// SyncContentTypeText is incomplete. It's an untested, partially stubbed method
// kept in place so we can start verifying flow and test with non-external data.
func SyncContentTypeText(c *model.ContentTypeText, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  if !c.ExternPath.IsValid() { // nothing to do
    return c, nil
  }

  externPath := c.ExternPath.String

  if externPath == `` {
    return nil, rest.ServerError(`Resource references empty external path.`, nil)
  }

  if c.SourceType.String == `NONE` {
    // Nothing to do.
    return c, nil
  } else {
    if c.SourceType.String == `URL` {
      if !httpRE.MatchString(externPath) {
        return nil, rest.ServerError(fmt.Sprintf(`URL-type resource references non-HTTP(S) URL: %s`, externPath), nil)
      }

      if response, err := http.Get(externPath); err != nil {
        return nil, rest.ServerError(fmt.Sprintf(`Could not retrieve external content from '%s'.`, externPath), err)
      } else {
        defer response.Body.Close()
        if body, err := ioutil.ReadAll(response.Body); err != nil {
          return nil, rest.ServerError(fmt.Sprintf(`Could not read external content body from '%s'.`, externPath), err)
        } else {
          c.Text = nulls.NewString(fmt.Sprintf(`%s`, body))
          c.LastSync = nulls.NewInt64(0) // will be updated by trigger
        }
      }
    // c.SourceType == `URL
    } else if c.SourceType.String == `GITLAB` {
      rest.ServerError(`GitLab content sync not yet implemented.`, nil)
    // c.SourceType == `GITLAB`
    } else {
      return nil, rest.ServerError(fmt.Sprintf(`Failed to sync content with unknown source type: '%s'`, c.SourceType), nil)
    }

    if newC, err := UpdateContentTypeText(c, ctx); err != nil {
      return nil, err
    } else {
      return newC, nil
    }
  }
}

// SyncContentSource is incomplete. It's an untested, partially stubbed method
// kept in place so we can start testing flow with non-external Content.
func SyncContentSource(cs *model.ContentSource, ctx context.Context) (*model.ContentSource, rest.RestError) {
  // TODO: the current logic could be inconsistent as it uses 'master' which
  // may change as the files are processed. To avoid this, we should start by
  // getting the current master commit ref and then use that in all subsequent
  // calls.
  if cs.SourceType.String == `GITLAB` {
    apiHost := cs.Config[`apiHost`]
    if apiHost.IsEmpty() {
      return nil, rest.ServerError(`Failed to sync GitLab source; no 'apiHost' configuration found.`, nil)
    }
    apiToken := cs.Config[`apiToken`]
    projectID := cs.Config[`projectID`]
    if projectID.IsEmpty() {
      return nil, rest.ServerError(`Failed to sync GitLab source; no 'projectID' configuration found.`, nil)
    }
    contentPath := cs.Config[`contentPath`]

    recursive := true
    git := gitlab.NewClient(nil, apiToken.String)
    git.SetBaseURL(`https://` + apiHost.String + `/api/v4`)
    var lastResponse *gitlab.Response = nil
    pathCommitMap := make(map[string]string)
    listTreeOptions := &gitlab.ListTreeOptions{
      ListOptions : gitlab.ListOptions{ Page: 0, PerPage: 100 },
      Recursive   : &recursive,
    }
    // build out the 'pathCommitMap'
    for lastResponse == nil || lastResponse.NextPage != 0 {
      treeNodes, lastResponse, err := git.Repositories.ListTree(projectID.String, listTreeOptions)
      if err != nil {
        // TODO: check response and return appropriate error type
        return nil, rest.ServerError(fmt.Sprintf(`Problem while retrieving GitLab tree for project '%s' from '%s'.`, projectID, apiHost), err)
      }

      for _, treeNode := range treeNodes {
        path := treeNode.Path
        var ref string = `master`
        if treeNode.Type == `blob` && (contentPath.IsEmpty() || strings.HasPrefix(path, contentPath.String)) {
          // TODO: pay attention to response?
          fileMetaData, _, err := git.RepositoryFiles.GetFileMetaData(projectID.String, path, &gitlab.GetFileMetaDataOptions{&ref})
          if err != nil {
            // TODO: check response and return appropriate error type
            return nil, rest.ServerError(fmt.Sprintf(`Problem while retrieving GitLab file '%s' for project '%s' from '%s'.`, treeNode.Path, projectID, apiHost), err)
          }

          pathCommitMap[path] = fileMetaData.CommitID
        }
      }

      listTreeOptions.Page = lastResponse.NextPage
    }

    // new we have the 'pathCommitMap', we go to the DB and select everything
    currentRecordsQuery := `SELECT cs.id, cs.extern_path, cs.version_cookie FROM content_summary cs JOIN namespace ns ON cs.namespace=ns.id WHERE ns.name=?`

    rows, err := sqldb.DB.QueryContext(ctx, currentRecordsQuery, cs.Name)
    if err != nil {
      return nil, rest.ServerError(`Problem while gathering current records.`, err)
    }

    for rows.Next() {
      var id int64
      var externPath, versionCookie string
      rows.Scan(&id, &externPath, &versionCookie)


      /*
      currentVersion, valid := pathCommitMap[externPath]
      if !valid { // then we have a record for something that no longer exists
        if err := DeleteContentTypeTextByID(*id) {
          return nil, rest.ServerError(fmt.Sprintf(`Problem deleting stale record for %s/%s.`, cs.ContentNamespace.Name, externPath), err)
        }
      } else if currentVersion != versionCookie {
        ctt := &model.ContentTypeText{

        }
      }*/
    }
  }

  return cs, nil
}
