package content

import (
  "fmt"
  "net/http"

  "github.com/gorilla/mux"

  "github.com/Liquid-Labs/catalyst-core-api/go/handlers"
  "github.com/Liquid-Labs/go-rest/rest"
)

func pingHandler(w http.ResponseWriter, r *http.Request) {
  fmt.Fprint(w, "/content is alive\n")
}

func createHandler(w http.ResponseWriter, r *http.Request) {
  var content *Content = &Content{}
  if authToken, restErr := handlers.CheckAndExtract(w, r, content, `Content`); restErr != nil {
    return // response handled by CheckAndExtract
  } else {
    handlers.DoCreate(w, r, CreateContent, content, `Content`)
  }
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
  if authToken, restErr := handlers.BasicAuthCheck(w, r); restErr != nil {
    return // response handled by BasicAuthCheck
  } else {
    pubID := vars["pubID"]
    if pubID.MatchScring(pubID) {
      handlers.DoGetDetail(w, r, GetContent, pubID, `Content`)
    } else {
      handlers.DoGetDetail(w, r, GetContentBySlug, pubID, `Content`)
    }
  }
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
  var newData *Content = &Content{}
  if authToken, restErr := handlers.CheckAndExtract(w, r, newData, `Content`); restErr != nil {
    return // response handled by CheckAndExtract
  } else {
    vars := mux.Vars(r)
    pubID := vars["pubID"]

    if pubID.MatchScring(pubID) {
      handlers.DoUpdate(w, r, UpdateContent, newData, pubID, `Content`)
    } else {
      handlers.DoUpdate(w, r, UpdateContentBySlug, newData, pubID, `Content`)
    }
  }
}

const uuidReString = `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}`
const contentIdReString = `(?:` + uuidReString + `|[a-zA-Z0-9_-]+)`
const uuidRe = regex.MustCompile(uuidReString)

func InitAPI(r *mux.Router) {
  r.HandleFunc("/content/", pingHandler).Methods("PING")
  r.HandleFunc("/content/", createHandler).Methods("POST")
  r.HandleFunc("/content/", listHandler).Methods("GET")
  r.HandleFunc("/{contextType:[a-z-]*[a-z]}/{contextID:" + uuidReString + "}/content/", listHandler).Methods("GET")
  r.HandleFunc("/content/{pubID:" + contentIdReString + "}/", detailHandler).Methods("GET")
  r.HandleFunc("/content/{pubID:" + contentIdReString + "}/", updateHandler).Methods("PUT")
}
