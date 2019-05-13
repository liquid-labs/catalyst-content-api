package content

import (
  "context"
  "fmt"
  "ioutil"
  "net/http"
  "regexp"
  "time"

  "github.com/Liquid-Labs/go-rest/rest"
)

const relativePath = regexp.MustCompile(`^[a-zA-Z0-9_-][a-zA-Z0-9/_-]*`)
const httpRe = regexp.MustCompile(`^https?://`)

func (c *ContentTypeText) SyncContentTypeText(ctx context.Context) *ContentTypeText, rest.RestError {
  if c.ExternPath.IsNull { // nothing to do
    return c, nil
  }

  externPath := c.ExternPath.String

  if externPath == '' {
    return nil, rest.ServerError(`Resource references empty external path.`, nil)
  }

  if relativePath.MatchString(externPath) {
    baseContentPath := os.Getenv(`BASE_CONTENT_PATH`)
    if baseContentPath == '' {
      return nil, rest.ServerError(`No 'BASE_CONTENT_PATH' set. Attempt to synchronize relative external path failed.`, nil)
    } else {
      externPath = baseContentPath + externPath
    }
  }

  if !httpRe.MatchString(externPath) {
    return nil, rest.ServerError(fmt.Sprintf(`Resource references non-HTTP(S) URL: %s`, externPath), nil)
  }

  if response, err := http.Get(externPath); err != nil {
    return nil, new ServerError(fmt.Sprintf(`Could not retrieve external content from '%s'.`, externPath), err)
  } else {
    defer response.Body.Close()
    if body, err := ioutil.ReadAll(resp.Body); err != nil {
      return nil, rest.ServerError(fmt.Sprintf(`Could not read external content body from '%s'.`, externPath), err)
    } else {
      c.Text = nulls.NewString(body)
      c.LastSync = 0 // will be updated by trigger

      if newC, err := UpdateContentTextType(c, ctx); err != nil {
        return nil, err
      } else {
        return newC, nil
      }
    }
  }
}
