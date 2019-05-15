package content

import (
  "context"
  "fmt"
  "io/ioutil"
  "os"
  "net/http"
  "regexp"

  "github.com/Liquid-Labs/go-nullable-mysql/nulls"
  "github.com/Liquid-Labs/go-rest/rest"

  model "github.com/Liquid-Labs/catalyst-content-model/go/resources/content"
)

var relativePath, httpRE *regexp.Regexp =
  regexp.MustCompile(`^[a-zA-Z0-9_-][a-zA-Z0-9/_-]*`),
  regexp.MustCompile(`^https?://`)

func SyncContentTypeText(c *model.ContentTypeText, ctx context.Context) (*model.ContentTypeText, rest.RestError) {
  if !c.ExternPath.IsValid() { // nothing to do
    return c, nil
  }

  externPath := c.ExternPath.String

  if externPath == `` {
    return nil, rest.ServerError(`Resource references empty external path.`, nil)
  }

  if relativePath.MatchString(externPath) {
    baseContentPath := os.Getenv(`CONTENT_BASE_PATH`)
    if baseContentPath == `` {
      return nil, rest.ServerError(`No 'CONTENT_BASE_PATH' set. Attempt to synchronize relative external path failed.`, nil)
    } else {
      externPath = baseContentPath + externPath
    }
  }

  if !httpRE.MatchString(externPath) {
    return nil, rest.ServerError(fmt.Sprintf(`Resource references non-HTTP(S) URL: %s`, externPath), nil)
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

      if newC, err := UpdateContentTypeText(c, ctx); err != nil {
        return nil, err
      } else {
        return newC, nil
      }
    }
  }
}
