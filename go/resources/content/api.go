package content

import (
  "fmt"
  "net/http"
  "os"
  "regexp"

  "github.com/gorilla/mux"

  "github.com/Liquid-Labs/catalyst-core-api/go/handlers"
  "github.com/Liquid-Labs/go-rest/rest"

  model "github.com/Liquid-Labs/catalyst-content-model/go/resources/content"
)

func pingHandler(w http.ResponseWriter, r *http.Request) {
  fmt.Fprint(w, "/content is alive\n")
}

func createHandler(w http.ResponseWriter, r *http.Request) {
  contentSummary := &model.ContentSummary{}
  if _, restErr := handlers.CheckAndExtract(w, r, contentSummary, `ContentSumamry`); restErr != nil {
    return // response handled by CheckAndExtract
  }

  var data interface{}
  var restErr rest.RestError
  switch contentSummary.Type.String {
  case `TEXT`: {
    content := &model.ContentTypeText{}
    if restErr = rest.ExtractJson(w, r, content, `ContentTypeText`); restErr != nil {
      return // response handled by CheckAndExtract
    } else {
      data, restErr = CreateContentTypeText(content, r.Context())
    }
  }
  default:
    rest.HandleError(w, rest.BadRequestError(fmt.Sprintf(`Invalid content type: '%s'`, contentSummary.Type.String), nil))
  }

  handlers.ProcessGenericResults(w, r, data, restErr, `Creating Content.`)
}

func listHandler(w http.ResponseWriter, r *http.Request) {
  vars := mux.Vars(r)
  contextType := vars["contextType"]

  if contextType == "" {
    fmt.Fprint(w, "TODO: listing contents")
  } else {
    contextType := vars["contextType"]
    contextId := vars["contextId"]
    // TODO: distribute 'join' defs as common includes to all resources in a
    // given system; e.g., list is compiled at app.
    // TODO: make internal REST call to get the 'JOIN' info for unknowns?
    fmt.Fprintf(w, "TODO: in context %s/%s\n", contextType, contextId)
  }
}

func detailHandler(w http.ResponseWriter, r *http.Request) {
  if _, restErr := handlers.BasicAuthCheck(w, r); restErr != nil {
    return // response handled by BasicAuthCheck
  } else {
    // TODO: support multiple types
    pubID := mux.Vars(r)["pubID"]
    var err rest.RestError
    var result *model.ContentTypeText
    if uuidRe.MatchString(pubID) {
      result, err = GetContentTypeText(pubID, r.Context())
    } else {
      namespace := os.Getenv(`CONTENT_NAMESPACE`)
      if namespace == `` {
        rest.HandleError(w, rest.ServerError(`No 'CONTENT_NAMESPACE' defined while attempting to retrieve content by slug.`, nil))
        return
      }
      result, err = GetContentTypeTextByNSSlug(namespace, pubID, r.Context())
    }
    handlers.ProcessGenericResults(w, r, result, err, `Retrieve Content.`)
  }
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
  newContent := &model.ContentSummary{}
  if _, restErr := handlers.CheckAndExtract(w, r, newContent, `ContentSummary`); restErr != nil {
    return // response handled by CheckAndExtract
  } else {
    contentType := newContent.GetType()
    pubID := mux.Vars(r)["pubID"]

    var data interface{}
    switch contentType.String {
    case `TEXT`: {
      ctt := &model.ContentTypeText{}
      if restErr = rest.ExtractJson(w, r, data, `ContentTypeText`); restErr != nil {
        return // already handled
      }

      // run the URL-entity PubID match check if we can
      if uuidRe.MatchString(pubID) && !handlers.CheckUpdateByPubID(w, pubID, ctt) {
        return
      }

      data, restErr = UpdateContentTypeText(ctt, r.Context())
    }
    default:
      rest.HandleError(w, rest.BadRequestError(fmt.Sprintf(`Unknown content type: '%s'`, contentType), nil))
    }
    handlers.ProcessGenericResults(w, r, data, restErr, `Content updated.`)
  }
}

const uuidReString = `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}`
const contentIdReString = `(?:` + uuidReString + `|[a-zA-Z0-9_-]+)`
var uuidRe *regexp.Regexp = regexp.MustCompile(uuidReString)

func InitAPI(r *mux.Router) {
  r.HandleFunc("/content/", pingHandler).Methods("PING")
  r.HandleFunc("/content/", createHandler).Methods("POST")
  r.HandleFunc("/content/", listHandler).Methods("GET")
  r.HandleFunc("/{contextType:[a-z-]*[a-z]}/{contextID:" + uuidReString + "}/content/", listHandler).Methods("GET")
  r.HandleFunc("/content/{pubID:" + contentIdReString + "}/", detailHandler).Methods("GET")
  r.HandleFunc("/content/{pubID:" + contentIdReString + "}/", updateHandler).Methods("PUT")
}
